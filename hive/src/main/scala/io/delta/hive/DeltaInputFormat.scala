package io.delta.hive

import java.io.IOException

import io.delta.thin.DeltaLog
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat
import org.apache.hadoop.mapred.FileInputFormat._
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.security.TokenCache
import org.apache.spark.sql.SparkSession

@UseFileSplitsFromInputFormat
class DeltaInputFormat extends MapredParquetInputFormat {

  import DeltaInputFormat._

  @throws[IOException]
  override def listStatus(job: JobConf): Array[FileStatus] = try {
    val dirs = getInputPaths(job)
    if (dirs.isEmpty) {
      throw new IOException("No input paths specified in job")
    } else {
      TokenCache.obtainTokensForNamenodes(job.getCredentials, dirs, job)

      // find delta root path
      val rootPath = DeltaLog.findDeltaTableRoot(dirs.head).get
      val deltaLog = DeltaLog.forTable(rootPath.toString)
      // get the snapshot of the version
      val snapshotToUse = deltaLog.snapshot

      val fs = rootPath.getFileSystem(job)

      // get partition filters
      val partitionFragments = dirs.map { dir =>
          val relativePath = DeltaInputFormat.tryRelativizePath(fs, rootPath, dir)
          assert(
            !relativePath.isAbsolute,
            s"Fail to relativize path $dir against base path $rootPath.")
          relativePath.toUri.toString
        }

      partitionFragments.foreach(println(_))
      snapshotToUse.allFiles.filter(x => partitionFragments.exists(x.path.contains(_))).map { file =>
        fs.getFileStatus(new Path(rootPath, file.path))
      }.toArray
      // selected files to Hive to be processed
//      DeltaLog.filterFileList(
//        snapshotToUse.metadata.partitionColumns, snapshotToUse.allFiles.toDF(), partitionFilters)
//        .as[AddFile](SingleAction.addFileEncoder)
//        .collect().map { file =>
//          fs.getFileStatus(new Path(rootPath, file.path))}
    }
  } catch {
    case e: Throwable =>
      e.printStackTrace()
      throw e
  }
}

object DeltaInputFormat {

  def tryRelativizePath(fs: FileSystem, basePath: Path, child: Path): Path = {
    // Child Paths can be absolute and use a separate fs
    val childUri = child.toUri
    // We can map multiple schemes to the same `FileSystem` class, but `FileSystem.getScheme` is
    // usually just a hard-coded string. Hence, we need to use the scheme of the URI that we use to
    // create the FileSystem here.
    if (child.isAbsolute) {
      try {
        new Path(fs.makeQualified(basePath).toUri.relativize(fs.makeQualified(child).toUri))
      } catch {
        case e: IllegalArgumentException =>
          throw new IllegalStateException(
            s"""Failed to relativize the path ($child). This can happen when absolute paths make
               |it into the transaction log, which start with the scheme s3://, wasbs:// or adls://.
               |This is a bug that has existed before DBR 5.0. To fix this issue, please upgrade
               |your writer jobs to DBR 5.0 and please run:
               |%scala com.databricks.delta.Delta.fixAbsolutePathsInLog("$child")
             """.stripMargin)
      }
    } else {
      child
    }
  }

  def spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("HiveOnDelta Get Files")
    .getOrCreate()
}
