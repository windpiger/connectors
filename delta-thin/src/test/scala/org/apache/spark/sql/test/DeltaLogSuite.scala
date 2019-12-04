package org.apache.spark.sql.test

import java.io.{File, FileNotFoundException}

import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.DeltaOperations.Truncate
import org.apache.spark.sql.delta.actions.{Action, AddFile, RemoveFile}
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}
import org.apache.spark.sql.delta.{CheckpointInstance, DeltaLog, DeltaOperations}
import org.apache.spark.util.Utils

// scalastyle:off: removeFile
class DeltaLogSuite extends QueryTest
  with SharedSparkSession  with SQLTestUtils {

  protected val testOp = Truncate()

  testQuietly("checkpoint") {
    val tempDir = Utils.createTempDir()
    val log1 = DeltaLog(spark, new Path(tempDir.getCanonicalPath))

    (1 to 15).foreach { i =>
      val txn = log1.startTransaction()
      val file = AddFile(i.toString, Map.empty, 1, 1, true) :: Nil
      val delete: Seq[Action] = if (i > 1) {
        RemoveFile(i - 1 toString, Some(System.currentTimeMillis()), true) :: Nil
      } else {
        Nil
      }
      txn.commit(delete ++ file, testOp)
    }

    DeltaLog.clearCache()
    val log2 = DeltaLog(spark, new Path(tempDir.getCanonicalPath))
    assert(log2.snapshot.version == log1.snapshot.version)
    assert(log2.snapshot.allFiles.count == 1)

    val deltaLogThin = io.delta.thin.DeltaLog(new Path(tempDir.getCanonicalPath))
    assert(log2.snapshot.version == log1.snapshot.version)
    assert(log2.snapshot.allFiles.count == 1)


  }
}