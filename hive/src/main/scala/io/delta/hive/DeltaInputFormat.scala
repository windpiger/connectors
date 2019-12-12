package io.delta.hive

import java.io.IOException
import java.net.URI

import io.delta.thin.DeltaLog
import io.delta.thin.actions.AddFile
import org.apache.hadoop.fs._
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
      deltaLog.update()
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

      // The default value 128M is the same as the default value of
      // "spark.sql.files.maxPartitionBytes" in Spark. It's also the default parquet row group size
      // which is usually the best split size for parquet files.
      val blockSize = job.getLong("parquet.block.size", 128L * 1024 * 1024)

      partitionFragments.foreach(println(_))
      snapshotToUse.allFiles.filter(x => partitionFragments.exists(x.path.contains(_))).map { f =>
        toFileStatus(fs, rootPath, f, blockSize)
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


  /**
    * Convert an [[AddFile]] to Hadoop's [[FileStatus]].
    *
    * @param root the table path which will be used to create the real path from relative path.
    */
  private def toFileStatus(fs: FileSystem, root: Path, f: AddFile, blockSize: Long): FileStatus = {
    val status = new FileStatus(
      f.size, // length
      false, // isDir
      1, // blockReplication, FileInputFormat doesn't use this
      blockSize, // blockSize
      f.modificationTime, // modificationTime
      absolutePath(fs, root, f.path) // path
    )
    // We don't have `blockLocations` in `AddFile`. However, fetching them by calling
    // `getFileStatus` for each file is unacceptable because that's pretty inefficient and it will
    // make Delta look worse than a parquet table because of these FileSystem RPC calls.
    //
    // But if we don't set the block locations, [[FileInputFormat]] will try to fetch them. Hence,
    // we create a `LocatedFileStatus` with dummy block locations to save FileSystem RPC calls. We
    // lose the locality but this is fine today since most of storage systems are on Cloud and the
    // computation is running separately.
    //
    // An alternative solution is using "listStatus" recursively to get all `FileStatus`s and keep
    // those present in `AddFile`s. This is much cheaper and the performance should be the same as a
    // parquet table. However, it's pretty complicated as we need to be careful to avoid listing
    // unnecessary directories. So we decide to not do this right now.
    val dummyBlockLocations =
    Array(new BlockLocation(Array("localhost:50010"), Array("localhost"), 0, f.size))
    new LocatedFileStatus(status, dummyBlockLocations)
  }

  /**
    * Create an absolute [[Path]] from `child` using the `root` path if `child` is a relative path.
    * Return a [[Path]] version of child` if it is an absolute path.
    *
    * @param child an escaped string read from Delta's [[AddFile]] directly which requires to
    *              unescape before creating the [[Path]] object.
    */
  private def absolutePath(fs: FileSystem, root: Path, child: String): Path = {
    val p = new Path(new URI(child))
    if (p.isAbsolute) {
      fs.makeQualified(p)
    } else {
      new Path(root, p)
    }
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
