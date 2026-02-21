# Benchmark report template

## Metadata

- date:
- commit:
- author:
- environment:
  - os:
  - cpu:
  - ram:
  - jdk:
  - sbt:
  - spark:

## Core JMH throughput

Command:

```bash
sbt "jmh/jmh:run -i 5 -wi 5 -f1 -t1 io.toonformat.toon4s.jmh.EncodeDecodeBench"
```

Summary:

| benchmark | score | unit |
|---|---:|---|
| encode_object |  | ops ms |
| decode_tabular |  | ops ms |
| decode_list |  | ops ms |
| decode_nested |  | ops ms |
| encode_large_uniform |  | ops ms |
| encode_irregular |  | ops ms |
| encode_real_world |  | ops ms |

Raw output file:

- benchmark-jmh.txt

## Spark integration token and throughput

Dataset:

- name:
- rows:
- columns:
- max depth:

Command or notebook step:

- measureEncodingPerformance call:

Results:

| metric | value |
|---|---:|
| duration ms |  |
| throughput rows per second |  |
| json token count |  |
| toon token count |  |
| token savings percent |  |

## Comparison to previous report

| metric | previous | current | delta |
|---|---:|---:|---:|
| encode_real_world ops ms |  |  |  |
| spark token savings percent |  |  |  |

## Notes

- data changes:
- code changes:
- risks:

## Claim update decision

- README claim changed: yes or no
- claims traceability file updated: yes or no
