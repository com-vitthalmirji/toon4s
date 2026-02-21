package io.toonformat.toon4s.spark.datasource

import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.util

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
import scala.util.{Try, Using}

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
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration

/**
 * Third-party TOON DataSource V2.
 *
 * Read path: one TOON document per row in a single `toon` string column. Write path: one TOON
 * document chunks per task file, produced from input rows.
 */
class ToonDataSourceV2 extends TableProvider with DataSourceRegister {

  override def shortName(): String = "toon"

  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    ToonDataSourceV2.ReadSchema

  override def supportsExternalMetadata(): Boolean = true

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String],
  ): Table = {
    new ToonTable(
      options = new CaseInsensitiveStringMap(properties),
      userSchema = Option(schema),
    )
  }

}

private[datasource] object ToonDataSourceV2 {

  val ToonColumnName = "toon"

  val KeyOption = "key"

  val PathOption = "path"

  val DefaultTopLevelKey = "data"

  val MaxRowsPerFileOption = "maxRowsPerFile"

  val DefaultMaxRowsPerFile = 10000

  val ReadSchema: StructType =
    StructType(Seq(StructField(ToonColumnName, StringType, nullable = false)))

  val WriterFilePrefix = "part-"

  sealed trait DataSourceError {

    def message: String

  }

  final case class MissingRequiredOption(name: String) extends DataSourceError {

    override def message: String = s"Option '$name' is required for format(\"toon\")"

  }

  final case class InvalidOption(name: String, reason: String) extends DataSourceError {

    override def message: String = s"Option '$name' is invalid for format(\"toon\"): $reason"

  }

  final case class CommitFailure(message: String) extends DataSourceError

  def toException(error: DataSourceError): IllegalArgumentException =
    new IllegalArgumentException(error.message)

  def raise[A](error: DataSourceError): A =
    scala.util.Failure[A](toException(error)).get

  def raiseThrowable[A](error: Throwable): A =
    scala.util.Failure[A](error).get

  def activeHadoopConf(): Configuration = {
    val sessionResult = Try(SparkSession.active).toEither.left.map { ex =>
      CommitFailure(
        s"Active SparkSession is required for TOON datasource Hadoop configuration: ${ex.getMessage}"
      )
    }

    sessionResult
      .flatMap { session =>
        Try(session.sparkContext.hadoopConfiguration).toEither.left.map { ex =>
          CommitFailure(
            s"SparkContext Hadoop configuration is unavailable for TOON datasource: ${ex.getMessage}"
          )
        }
      }
      .fold(raise, identity)
  }

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
    val path = requirePath(options).fold(ToonDataSourceV2.raise, identity)
    val fs = path.getFileSystem(activeHadoopConf())
    val files = listVisibleFiles(fs, path)
    files.map(f => ToonInputPartition(f.toUri.toString)).toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new ToonPartitionReaderFactory(new SerializableConfiguration(activeHadoopConf()))
  }

  private def requirePath(
      map: CaseInsensitiveStringMap
  ): Either[ToonDataSourceV2.DataSourceError, Path] = {
    Option(map.get(PathOption)) match {
    case Some(v) if v.nonEmpty => Right(new Path(v))
    case _                     => Left(ToonDataSourceV2.MissingRequiredOption(PathOption))
    }
  }

  private def activeHadoopConf(): Configuration =
    ToonDataSourceV2.activeHadoopConf()

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

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val path = partition match {
    case ToonInputPartition(value) => new Path(value)
    case _                         =>
      ToonDataSourceV2.raise(
        ToonDataSourceV2.InvalidOption("partition", "unsupported partition type")
      )
    }
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
          readLines(reader, sb, isFirstLine = true)
          content = sb.result()
          consumed = true
          content.nonEmpty
        }
      }

      override def get(): InternalRow = InternalRow(UTF8String.fromString(content))

      override def close(): Unit = {
        val readerClose = Try(reader.close())
        val streamClose = Try(stream.close())
        readerClose.failed.foreach(ToonDataSourceV2.raiseThrowable[Unit])
        streamClose.failed.foreach(ToonDataSourceV2.raiseThrowable[Unit])
      }
    }
  }

  @tailrec
  private def readLines(
      reader: BufferedReader,
      builder: StringBuilder,
      isFirstLine: Boolean,
  ): Unit = {
    Option(reader.readLine()) match {
    case Some(line) =>
      if (!isFirstLine) {
        builder.append('\n')
      }
      builder.append(line)
      readLines(reader, builder, isFirstLine = false)
    case None =>
      ()
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
    val buildResult = for {
      outputPath <- requirePath(sourceOptions)
      maxRowsPerFile <- parsePositiveInt(
        sourceOptions,
        ToonDataSourceV2.MaxRowsPerFileOption,
        ToonDataSourceV2.DefaultMaxRowsPerFile,
      )
    } yield (outputPath, maxRowsPerFile)

    buildResult match {
    case Left(error) =>
      new Write {
        override def toBatch: BatchWrite = ToonDataSourceV2.raise(error)
      }
    case Right((outputPath, maxRowsPerFile)) =>
      val key = Option(sourceOptions.get(ToonDataSourceV2.KeyOption))
        .filter(_.nonEmpty)
        .getOrElse(ToonDataSourceV2.DefaultTopLevelKey)
      val schema = info.schema()
      val queryId = info.queryId()
      val hadoopConf = ToonDataSourceV2.activeHadoopConf()

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
            maxRowsPerFile = maxRowsPerFile,
            schema = schema,
            conf = hadoopConf,
          )
        }
      }
    }
  }

  private def requirePath(
      map: CaseInsensitiveStringMap
  ): Either[ToonDataSourceV2.DataSourceError, Path] = {
    Option(map.get(ToonDataSourceV2.PathOption)) match {
    case Some(v) if v.nonEmpty => Right(new Path(v))
    case _ => Left(ToonDataSourceV2.MissingRequiredOption(ToonDataSourceV2.PathOption))
    }
  }

  private def parsePositiveInt(
      map: CaseInsensitiveStringMap,
      key: String,
      defaultValue: Int,
  ): Either[ToonDataSourceV2.DataSourceError, Int] = {
    Option(map.get(key)).filter(_.nonEmpty) match {
    case None      => Right(defaultValue)
    case Some(raw) =>
      raw.toIntOption match {
      case Some(value) if value > 0 => Right(value)
      case _ => Left(ToonDataSourceV2.InvalidOption(key, "must be a positive integer"))
      }
    }
  }

}

final private[datasource] class ToonBatchWrite(
    queryId: String,
    outputPath: String,
    key: String,
    maxRowsPerFile: Int,
    schema: StructType,
    conf: Configuration,
) extends BatchWrite {

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    new ToonDataWriterFactory(
      outputPath = outputPath,
      queryId = queryId,
      key = key,
      maxRowsPerFile = maxRowsPerFile,
      schema = schema,
      conf = new SerializableConfiguration(conf),
    )
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    val finalPath = new Path(outputPath)
    val tempPath = new Path(new Path(finalPath, "_temporary"), queryId)
    val fs = finalPath.getFileSystem(conf)
    if (!fs.exists(tempPath)) return

    val moveResult = Try {
      fs.listStatus(tempPath).foreach { part =>
        val src = part.getPath
        val dst = new Path(finalPath, src.getName)
        if (!fs.rename(src, dst)) {
          ToonDataSourceV2.raise[Unit](
            ToonDataSourceV2.CommitFailure(s"Failed to rename $src to $dst")
          )
        }
      }
    }.toEither

    val cleanupResult = Try {
      fs.delete(tempPath, true)
    }.toEither

    moveResult.left.foreach(ToonDataSourceV2.raiseThrowable[Unit])
    cleanupResult.left.foreach(ToonDataSourceV2.raiseThrowable[Unit])
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    val tempPath = new Path(new Path(outputPath, "_temporary"), queryId)
    val fs = tempPath.getFileSystem(conf)
    Try(fs.delete(tempPath, true)).failed.foreach(ToonDataSourceV2.raiseThrowable[Unit])
  }

}

final private[datasource] class ToonDataWriterFactory(
    outputPath: String,
    queryId: String,
    key: String,
    maxRowsPerFile: Int,
    schema: StructType,
    conf: SerializableConfiguration,
) extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    val jobPath = new Path(new Path(outputPath, "_temporary"), queryId)
    val fs = jobPath.getFileSystem(conf.value)
    fs.mkdirs(jobPath)
    new ToonDataWriter(fs, jobPath, partitionId, taskId, key, maxRowsPerFile, schema)
  }

}

final private[datasource] case class ToonWriterCommitMessage(files: Array[String])
    extends WriterCommitMessage

final private[datasource] class ToonDataWriter(
    fs: FileSystem,
    jobPath: Path,
    partitionId: Int,
    taskId: Long,
    key: String,
    maxRowsPerFile: Int,
    schema: StructType,
) extends DataWriter[InternalRow] {

  private val scalaConverter = CatalystTypeConverters.createToScalaConverter(schema)

  private val rows = scala.collection.mutable.ArrayBuffer.empty[JsonValue]

  private val createdFiles = scala.collection.mutable.ArrayBuffer.empty[Path]

  private var chunkIndex = 0

  override def write(record: InternalRow): Unit = {
    scalaConverter(record) match {
    case row: org.apache.spark.sql.Row =>
      rows += SparkJsonInterop.rowToJsonValue(row, schema)
      if (rows.size >= maxRowsPerFile) {
        flushChunk()
      }
    case _ =>
      ToonDataSourceV2.raise[Unit](
        ToonDataSourceV2.InvalidOption("row", "unsupported row conversion result")
      )
    }
  }

  override def commit(): WriterCommitMessage = {
    flushChunk()
    ToonWriterCommitMessage(createdFiles.map(_.toString).toArray)
  }

  override def abort(): Unit = {
    createdFiles.foreach(path => fs.delete(path, false))
    rows.clear()
  }

  override def close(): Unit = {}

  private def flushChunk(): Unit = {
    if (rows.isEmpty) {
      ()
    } else {
      val file = nextFilePath()
      chunkIndex += 1
      Using.resource(fs.create(file, true)) { out =>
        val payload = JObj(scala.collection.immutable.VectorMap(key -> JArray(rows.toVector)))
        val encoded =
          Toon.encode(payload, EncodeOptions()).fold(ToonDataSourceV2.raiseThrowable, identity)
        out.write(encoded.getBytes(StandardCharsets.UTF_8))
        createdFiles += file
        rows.clear()
      }
    }
  }

  private def nextFilePath(): Path = {
    new Path(jobPath, s"${ToonDataSourceV2.WriterFilePrefix}$partitionId-$taskId-$chunkIndex.toon")
  }

}
