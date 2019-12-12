package io.delta.hive

import java.io.IOException
import java.util.Properties

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.ql.exec.FileSinkOperator
import org.apache.hadoop.hive.ql.io.HiveOutputFormat
import org.apache.hadoop.io.{ArrayWritable, NullWritable, Writable}
import org.apache.hadoop.mapred.{JobConf, RecordWriter}
import org.apache.hadoop.util.Progressable

/**
 * This class is not a real implementation. We use it to prevent from writing to a Delta table in
 * Hive before we support it.
 */
class DeltaOutputFormat extends HiveOutputFormat[NullWritable, ArrayWritable] {

  private def writingNotSupported[T](): T = {
    throw new UnsupportedOperationException(
      "Writing to a Delta table in Hive is not supported. Please use Spark to write.")
  }

  @throws[IOException]
  override def getHiveRecordWriter(
    jc: JobConf,
    finalOutPath: Path,
    valueClass: Class[_ <: Writable],
    isCompressed: Boolean,
    tableProperties: Properties,
    progress: Progressable): FileSinkOperator.RecordWriter = writingNotSupported()

  override def getRecordWriter(
    ignored: FileSystem,
    job: JobConf,
    name: String,
    progress: Progressable): RecordWriter[NullWritable, ArrayWritable] = writingNotSupported()

  override def checkOutputSpecs(ignored: FileSystem, job: JobConf): Unit = writingNotSupported()
}