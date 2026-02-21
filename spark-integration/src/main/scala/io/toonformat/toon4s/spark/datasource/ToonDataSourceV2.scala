package io.toonformat.toon4s.spark.datasource

import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.util

import scala.jdk.CollectionConverters._

import io.toonformat.toon4s.{EncodeOptions, Toon}
import io.toonformat.toon4s.JsonValue
import io.toonformat.toon4s.JsonValue.{JArray, JObj}
import io.toonformat.toon4s.spark.SparkJsonInterop
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{
  SupportsRead,
  SupportsWrite,
  Table,
  TableCapability,
  TableProvider,
}
import org.apache.spark.sql.connector.catalog.TableCapability.{BATCH_READ, BATCH_WRITE, TRUNCATE}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{
  Batch,
  InputPartition,
  PartitionReader,
  PartitionReaderFactory,
  Scan,
  ScanBuilder,
}
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.internal.connector.SimpleTableProvider
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration

/**
 * Third-party TOON DataSource V2.
 *
 * Read path: one TOON document per row in a single `toon` string column. Write path: one TOON
 * document per task file, produced from input rows.
 */
class ToonDataSourceV2 extends SimpleTableProvider with DataSourceRegister {

  override def shortName(): String = "toon"

  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    ToonDataSourceV2.ReadSchema

  override def supportsExternalMetadata(): Boolean = true

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String],
  ): Table = {
    new ToonTable(new CaseInsensitiveStringMap(properties), Some(schema))
  }

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    new ToonTable(options, None)
  }

}

private[datasource] object ToonDataSourceV2 {

  val ToonColumnName = "toon"

  val KeyOption = "key"

  val PathOption = "path"

  val DefaultTopLevelKey = "data"

  val ReadSchema: StructType =
    StructType(Seq(StructField(ToonColumnName, StringType, nullable = false)))

  val WriterFilePrefix = "part-"

}

final private[datasource] class ToonTable(
    options: CaseInsensitiveStringMap,
    userSchema: Option[StructType],
) extends Table
    with SupportsRead
    with SupportsWrite {

  import ToonDataSourceV2._

  override def name(): String = "toon"

  override def schema(): StructType = userSchema.getOrElse(ReadSchema)

  override def capabilities(): util.Set[TableCapability] =
    util.EnumSet.of(BATCH_READ, BATCH_WRITE, TRUNCATE)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new ToonScanBuilder(options)

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    new ToonWriteBuilder(options, info)

}

final private[datasource] class ToonScanBuilder(options: CaseInsensitiveStringMap)
    extends ScanBuilder
    with Scan
    with Batch {

  import ToonDataSourceV2._

  override def build(): Scan = this

  override def readSchema(): StructType = ReadSchema

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    val path = requirePath(options)
    val fs = path.getFileSystem(activeHadoopConf())
    val files = listVisibleFiles(fs, path)
    files.map(f => ToonInputPartition(f.toUri.toString)).toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new ToonPartitionReaderFactory(new SerializableConfiguration(activeHadoopConf()))
  }

  private def requirePath(map: CaseInsensitiveStringMap): Path = {
    Option(map.get(PathOption)) match {
    case Some(v) if v.nonEmpty => new Path(v)
    case _                     =>
      throw new IllegalArgumentException(s"Option '$PathOption' is required for format(\"toon\")")
    }
  }

  private def activeHadoopConf(): Configuration =
    SparkSession.active.sparkContext.hadoopConfiguration

  private def listVisibleFiles(fs: FileSystem, root: Path): Seq[Path] = {
    if (!fs.exists(root)) {
      Seq.empty
    } else {
      val statuses = fs.listStatus(root)
      statuses.toSeq
        .filterNot(hidden)
        .flatMap {
          case st if st.isDirectory =>
            fs.listStatus(st.getPath).toSeq.filterNot(hidden)
          case st =>
            Seq(st)
        }
        .filterNot(_.isDirectory)
        .map(_.getPath)
    }
  }

  private def hidden(status: FileStatus): Boolean = {
    val name = status.getPath.getName
    name.startsWith("_") || name.startsWith(".")
  }

}

final private[datasource] case class ToonInputPartition(path: String) extends InputPartition

final private[datasource] class ToonPartitionReaderFactory(conf: SerializableConfiguration)
    extends PartitionReaderFactory {

  import ToonDataSourceV2._

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val input = partition.asInstanceOf[ToonInputPartition]
    val path = new Path(input.path)
    val fs = path.getFileSystem(conf.value)

    new PartitionReader[InternalRow] {
      private val stream = fs.open(path)
      private val reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))
      private var consumed = false
      private var content = ""

      override def next(): Boolean = {
        if (consumed) {
          false
        } else {
          val sb = new StringBuilder
          var line = reader.readLine()
          var first = true
          while (line != null) {
            if (!first) sb.append('\n')
            sb.append(line)
            first = false
            line = reader.readLine()
          }
          content = sb.result()
          consumed = true
          content.nonEmpty
        }
      }

      override def get(): InternalRow = InternalRow(UTF8String.fromString(content))

      override def close(): Unit = {
        try reader.close()
        finally stream.close()
      }
    }
  }

}

final private[datasource] class ToonWriteBuilder(
    sourceOptions: CaseInsensitiveStringMap,
    info: LogicalWriteInfo,
) extends WriteBuilder
    with SupportsTruncate {

  private var truncateTarget = false

  override def truncate(): WriteBuilder = {
    truncateTarget = true
    this
  }

  override def build(): Write = {
    val outputPath = requirePath(sourceOptions)
    val key = Option(sourceOptions.get(ToonDataSourceV2.KeyOption))
      .filter(_.nonEmpty)
      .getOrElse(ToonDataSourceV2.DefaultTopLevelKey)
    val schema = info.schema()
    val queryId = info.queryId()
    val hadoopConf = SparkSession.active.sparkContext.hadoopConfiguration

    new Write {
      override def toBatch: BatchWrite = {
        val fs = outputPath.getFileSystem(hadoopConf)
        if (truncateTarget) {
          fs.delete(outputPath, true)
        }
        new ToonBatchWrite(
          queryId = queryId,
          outputPath = outputPath.toUri.toString,
          key = key,
          schema = schema,
          conf = hadoopConf,
        )
      }
    }
  }

  private def requirePath(map: CaseInsensitiveStringMap): Path = {
    Option(map.get(ToonDataSourceV2.PathOption)) match {
    case Some(v) if v.nonEmpty => new Path(v)
    case _                     =>
      throw new IllegalArgumentException(
        s"Option '${ToonDataSourceV2.PathOption}' is required for format(\"toon\")"
      )
    }
  }

}

final private[datasource] class ToonBatchWrite(
    queryId: String,
    outputPath: String,
    key: String,
    schema: StructType,
    conf: Configuration,
) extends BatchWrite {

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    new ToonDataWriterFactory(
      outputPath = outputPath,
      queryId = queryId,
      key = key,
      schema = schema,
      conf = new SerializableConfiguration(conf),
    )
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    val finalPath = new Path(outputPath)
    val tempPath = new Path(new Path(finalPath, "_temporary"), queryId)
    val fs = finalPath.getFileSystem(conf)
    if (!fs.exists(tempPath)) return

    try {
      fs.listStatus(tempPath).foreach { part =>
        val src = part.getPath
        val dst = new Path(finalPath, src.getName)
        if (!fs.rename(src, dst)) {
          throw new RuntimeException(s"Failed to rename $src to $dst")
        }
      }
    } finally {
      fs.delete(tempPath, true)
    }
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    val tempPath = new Path(new Path(outputPath, "_temporary"), queryId)
    val fs = tempPath.getFileSystem(conf)
    fs.delete(tempPath, true)
  }

}

final private[datasource] class ToonDataWriterFactory(
    outputPath: String,
    queryId: String,
    key: String,
    schema: StructType,
    conf: SerializableConfiguration,
) extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    val jobPath = new Path(new Path(outputPath, "_temporary"), queryId)
    val filePath =
      new Path(jobPath, s"${ToonDataSourceV2.WriterFilePrefix}$partitionId-$taskId.toon")
    val fs = filePath.getFileSystem(conf.value)
    new ToonDataWriter(fs, filePath, key, schema)
  }

}

final private[datasource] class ToonDataWriter(
    fs: FileSystem,
    file: Path,
    key: String,
    schema: StructType,
) extends DataWriter[InternalRow] {

  private val scalaConverter = CatalystTypeConverters.createToScalaConverter(schema)

  private val rows = scala.collection.mutable.ArrayBuffer.empty[JsonValue]

  override def write(record: InternalRow): Unit = {
    val row = scalaConverter(record).asInstanceOf[org.apache.spark.sql.Row]
    rows += SparkJsonInterop.rowToJsonValue(row, schema)
  }

  override def commit(): WriterCommitMessage = {
    val payload = JObj(scala.collection.immutable.VectorMap(key -> JArray(rows.toVector)))
    val encoded = Toon.encode(payload, EncodeOptions()).fold(throw _, identity)
    val out = fs.create(file, true)
    try out.write(encoded.getBytes(StandardCharsets.UTF_8))
    finally out.close()
    null
  }

  override def abort(): Unit = {
    fs.delete(file, false)
  }

  override def close(): Unit = {}

}
