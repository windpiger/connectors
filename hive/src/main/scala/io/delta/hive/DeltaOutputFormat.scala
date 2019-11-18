package io.delta.hive

import java.io.IOException
import java.util.Properties

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.util.Progressable

class DeltaOutputFormat extends MapredParquetOutputFormat {

  @throws[IOException]
  override def getHiveRecordWriter(
      jobConf: JobConf,
      finalOutPath: Path,
      valueClass: Class[_ <: Writable],
      isCompressed: Boolean,
      tableProperties: Properties,
      progress: Progressable): org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter = {
      throw new RuntimeException("Do not support write to HiveOnDelta")
  }
}
