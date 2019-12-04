package org.apache.spark.sql.delta.thin

import java.io.{File, FileNotFoundException}

import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.DeltaOperations.Truncate
import org.apache.spark.sql.delta.actions.{Action, AddFile, RemoveFile}
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}
import org.apache.spark.sql.delta.{CheckpointInstance, CheckpointMetaData, DeltaLog, DeltaOperations}
import org.apache.spark.util.Utils
import io.delta.thin.{DeltaLog => DeltaThinLog}
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.test.{SQLTestUtils, SharedSparkSession}

// scalastyle:off: removeFile
class DeltaLogSuite extends QueryTest
  with SharedSparkSession  with SQLTestUtils with DeltaThinTest {

  protected val testOp = Truncate()

  testQuietly("checkpoint") {
    withTempDir { dir =>
      val log1 = DeltaLog(spark, new Path(dir.getCanonicalPath))

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
      val log2 = DeltaLog(spark, new Path(dir.getCanonicalPath))
      assert(log2.snapshot.version == log1.snapshot.version)
      assert(log2.snapshot.allFiles.count == 1)

      val logThin = DeltaThinLog(new Path(dir.getCanonicalPath))
      assert(logThin.snapshot.version == log1.snapshot.version)
      assert(logThin.snapshot.allFiles.size == 1)
    }
  }

  testQuietly("SC-8078: update deleted directory") {
    withTempDir { dir =>
      val path = new Path(dir.getCanonicalPath)
      val log = DeltaLog(spark, path)

      // Commit data so the in-memory state isn't consistent with an empty log.
      val txn = log.startTransaction()
      val files = (1 to 10).map(f => AddFile(f.toString, Map.empty, 1, 1, true))
      txn.commit(files, testOp)
      log.checkpoint()

      val logThin = DeltaThinLog(path)
      val fs = path.getFileSystem(spark.sessionState.newHadoopConf())
      fs.delete(path, true)

      val thrown = intercept[FileNotFoundException] {
        log.update()
      }
      assert(thrown.getMessage().contains("No delta log found"))


      val thrown1 = intercept[FileNotFoundException] {
        logThin.update()
      }
      assert(thrown1.getMessage().contains("No delta log found"))

    }
  }

  testQuietly("ActionLog cache should use the normalized path as key") {
    withTempDir { tempDir =>
      val dir = tempDir.getAbsolutePath.stripSuffix("/")
      assert(dir.startsWith("/"))
      val fs = new Path("/").getFileSystem(spark.sessionState.newHadoopConf())
      val samePaths = Seq(
        new Path(dir + "/foo"),
        new Path(dir + "/foo/"),
        new Path(fs.getScheme + ":" + dir + "/foo"),
        new Path(fs.getScheme + "://" + dir + "/foo")
      )
      val logs = samePaths.map(DeltaLog(spark, _))
      logs.foreach { log =>
        assert(log eq logs.head)
      }

      val thinLogs = samePaths.map(DeltaThinLog(_))
      thinLogs.foreach { log =>
        assert(log eq thinLogs.head)
      }
    }
  }

  testQuietly("handle corrupted '_last_checkpoint' file") {
    withTempDir { tempDir =>
      val log = DeltaLog(spark, new Path(tempDir.getCanonicalPath))

      val checkpointInterval = log.checkpointInterval
      for (f <- 0 to checkpointInterval) {
        val txn = log.startTransaction()
        txn.commit(Seq(AddFile(f.toString, Map.empty, 1, 1, true)), testOp)
      }
      assert(log.lastCheckpoint.isDefined)

      val lastCheckpoint = log.lastCheckpoint.get

      // Create an empty "_last_checkpoint" (corrupted)
      val fs = log.LAST_CHECKPOINT.getFileSystem(spark.sessionState.newHadoopConf)
      fs.create(log.LAST_CHECKPOINT, true /* overwrite */).close()

      // Create a new DeltaLog
      DeltaLog.clearCache()
      val log2 = DeltaLog(spark, new Path(tempDir.getCanonicalPath))
      // Make sure we create a new DeltaLog in order to test the loading logic.
      assert(log ne log2)

      // We should get the same metadata even if "_last_checkpoint" is corrupted.
      assert(CheckpointInstance(log2.lastCheckpoint.get) === CheckpointInstance(lastCheckpoint))

      val logThin = DeltaThinLog(new Path(tempDir.getCanonicalPath))
      val deltaThinCheckpointMeta = logThin.lastCheckpoint.get

      assert(CheckpointInstance(convertCheckpointMetaData(logThin.lastCheckpoint.get)) === CheckpointInstance(lastCheckpoint))
    }
  }

  testQuietly("paths should be canonicalized") {
    Seq("file:", "file://").foreach { scheme =>
      withTempDir { dir =>
        val log = DeltaLog.forTable(spark, dir)
        assert(new File(log.logPath.toUri).mkdirs())

        JavaUtils.deleteRecursively(dir)
        val logThin = DeltaThinLog.forTable(dir)
        assert(new File(logThin.logPath.toUri).mkdirs())

        val path = "/some/unqualified/absolute/path"
        val add = AddFile(
          path, Map.empty, 100L, 10L, dataChange = true)
        val rm = RemoveFile(
          s"$scheme$path", Some(200L), dataChange = false)

        log.store.write(
          FileNames.deltaFile(log.logPath, 0L),
          Iterator(JsonUtils.toJson(add.wrap)))
        log.store.write(
          FileNames.deltaFile(log.logPath, 1L),
          Iterator(JsonUtils.toJson(rm.wrap)))

        assert(log.update().version === 1)
        assert(log.snapshot.numOfFiles === 0)

        assert(logThin.update().version === 1)
        assert(logThin.snapshot.numOfFiles === 0)
      }
    }
  }

  testQuietly("paths should be canonicalized - special characters") {
    Seq("file:", "file://").foreach { scheme =>
      withTempDir { dir =>
        val log = DeltaLog.forTable(spark, dir)
        val logThin = DeltaThinLog.forTable(dir)

        assert(new File(log.logPath.toUri).mkdirs())
        val path = new Path("/some/unqualified/with space/p@#h").toUri.toString
        val add = AddFile(
          path, Map.empty, 100L, 10L, dataChange = true)
        val rm = RemoveFile(
          s"$scheme$path", Some(200L), dataChange = false)

        log.store.write(
          FileNames.deltaFile(log.logPath, 0L),
          Iterator(JsonUtils.toJson(add.wrap)))
        log.store.write(
          FileNames.deltaFile(log.logPath, 1L),
          Iterator(JsonUtils.toJson(rm.wrap)))

        assert(log.update().version === 1)
        assert(log.snapshot.numOfFiles === 0)

        assert(logThin.update().version === 1)
        assert(logThin.snapshot.numOfFiles === 0)
      }
    }
  }

  test("delete and re-add the same file in different transactions") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(spark, dir)
      val logThin = DeltaThinLog.forTable(dir)
      assert(new File(log.logPath.toUri).mkdirs())

      val add1 = AddFile("foo", Map.empty, 1L, System.currentTimeMillis(), dataChange = true)
      log.startTransaction().commit(add1 :: Nil, DeltaOperations.ManualUpdate)

      val rm = add1.remove
      log.startTransaction().commit(rm :: Nil, DeltaOperations.ManualUpdate)

      val add2 = AddFile("foo", Map.empty, 1L, System.currentTimeMillis(), dataChange = true)
      log.startTransaction().commit(add2 :: Nil, DeltaOperations.ManualUpdate)

      // Add a new transaction to replay logs using the previous snapshot. If it contained
      // AddFile("foo") and RemoveFile("foo"), "foo" would get removed and fail this test.
      val otherAdd = AddFile("bar", Map.empty, 1L, System.currentTimeMillis(), dataChange = true)
      log.startTransaction().commit(otherAdd :: Nil, DeltaOperations.ManualUpdate)

      assert(log.update().allFiles.collect().find(_.path == "foo")
        // `dataChange` is set to `false` after replaying logs.
        === Some(add2.copy(dataChange = false)))

      assert(Some(convertAddFile(logThin.update().allFiles.find(_.path == "foo").get))
        // `dataChange` is set to `false` after replaying logs.
        === Some(add2.copy(dataChange = false)))
    }
  }

  test("allFiles same with fat-delta") {
    withTempDir { dir =>
      val log1 = DeltaLog(spark, new Path(dir.getCanonicalPath))
      val logThin = DeltaThinLog(new Path(dir.getCanonicalPath))

      (1 to 15).foreach { i =>
        val txn = log1.startTransaction()
        val file = AddFile(i.toString, Map.empty, 1, 1, true) :: Nil
        val delete: Seq[Action] = if (i > 1) {
          RemoveFile(i - 1 toString, Some(System.currentTimeMillis()), true) :: Nil
        } else {
          Nil
        }
        txn.commit(delete ++ file, testOp)
        logThin.update()

        assert(logThin.snapshot.version == log1.snapshot.version)
        assert(logThin.snapshot.allFiles.size == 1)
        assert(convertAddFile(logThin.snapshot.allFiles.head) == log1.snapshot.allFiles.collect().head)
      }
    }
  }

  test("snapshot correct") {
    import testImplicits._

    val tableColumns = Seq("c1", "c2")
    withTempDir { dir =>
      val logThin = DeltaThinLog.forTable(dir.getCanonicalPath)
      val log = DeltaLog.forTable(spark, dir.getCanonicalPath)

      //STEP-1: insert data
      val testData = (0 until 10).map(x => (x, s"test-thin0-${x % 2}"))
      testData.toDS.toDF("c1", "c2").write.format("delta").save(dir.getCanonicalPath)
      val snapshotFiles_0 = dir.listFiles().filter(_.isFile).filter(_.getName.endsWith("snappy.parquet"))

      assert(logThin.snapshot.version == -1)
      logThin.update()
      assert(logThin.snapshot.version == 0)
      assert(logThin.snapshot.allFiles.forall { f =>
        snapshotFiles_0.exists(_.getName == f.path) })
      assert(snapshotFiles_0.length == logThin.snapshot.allFiles.size)
      assert(convertMetaData(logThin.snapshot.metadata) == log.snapshot.metadata)
      assert(convertProtocol(logThin.snapshot.protocol) == log.snapshot.protocol)

      //STEP-2: continue to insert data
      val testData1 = (0 until 10).map(x => (x, s"test-thin1-${x % 2}"))
      testData1.toDS.toDF("c1", "c2").write.mode("append").format("delta").save(dir.getCanonicalPath)
      val snapshotFiles_1 = dir.listFiles().filter(_.isFile).filter(_.getName.endsWith("snappy.parquet"))
      // snapshot is not changed before call update()
      assert(logThin.snapshot.version == 0)
      assert(logThin.snapshot.allFiles.forall { f =>
        snapshotFiles_0.exists(_.getName == f.path) })
      assert(snapshotFiles_0.length == logThin.snapshot.allFiles.size)

      // snapshot is changed after call update()
      logThin.update()
      assert(logThin.snapshot.version == 1)
      assert(logThin.snapshot.allFiles.forall { f =>
        snapshotFiles_1.exists(_.getName == f.path) })
      assert(snapshotFiles_1.length == logThin.snapshot.allFiles.size)
      assert(convertMetaData(logThin.snapshot.metadata) == log.snapshot.metadata)
      assert(convertProtocol(logThin.snapshot.protocol) == log.snapshot.protocol)

      //STEP-3: overwrite data
      val testData2 = (0 until 10).map(x => (x, s"test-thin2-${x % 2}"))
      testData2.toDS.toDF("c1", "c2").write.mode("overwrite").format("delta").save(dir.getCanonicalPath)
      // snapshot is not changed before call upate()
      assert(logThin.snapshot.version == 1)
      assert(logThin.snapshot.allFiles.forall { f =>
        snapshotFiles_1.exists(_.getName == f.path) })
      assert(snapshotFiles_1.length == logThin.snapshot.allFiles.size)

      // snapshot is changed after call update()
      logThin.update()
      val snapshotFiles_2 = dir.listFiles().filter(_.isFile).filter(_.getName.endsWith("snappy.parquet"))
        .filterNot(f => snapshotFiles_1.exists(_.getName == f.getName))
      assert(logThin.snapshot.version == 2)
      assert(logThin.snapshot.allFiles.forall { f =>
        snapshotFiles_2.exists(_.getName == f.path) })
      assert(snapshotFiles_2.length == logThin.snapshot.allFiles.size)
      assert(convertMetaData(logThin.snapshot.metadata) == log.snapshot.metadata)
      assert(convertProtocol(logThin.snapshot.protocol) == log.snapshot.protocol)

      //STEP-4: continue to insert data
      val testData3 = (0 until 10).map(x => (x, s"test-thin3-${x % 2}"))
      testData3.toDS.toDF("c1", "c2").write.mode("append").format("delta").save(dir.getCanonicalPath)
      val snapshotFiles_3 = dir.listFiles().filter(_.isFile).filter(_.getName.endsWith("snappy.parquet"))
        .filterNot(f => snapshotFiles_1.exists(_.getName == f.getName))
      // snapshot is not changed before call update()
      assert(logThin.snapshot.version == 2)
      assert(logThin.snapshot.allFiles.forall { f =>
        snapshotFiles_2.exists(_.getName == f.path) })
      assert(snapshotFiles_2.length == logThin.snapshot.allFiles.size)

      // snapshot is changed after call update()
      logThin.update()
      assert(logThin.snapshot.version == 3)
      assert(logThin.snapshot.allFiles.forall { f =>
        snapshotFiles_3.exists(_.getName == f.path) })
      assert(snapshotFiles_3.length == logThin.snapshot.allFiles.size)
      assert(convertMetaData(logThin.snapshot.metadata) == log.snapshot.metadata)
      assert(convertProtocol(logThin.snapshot.protocol) == log.snapshot.protocol)

      //STEP-5: delete data in step 3
      val deltaTable = DeltaTable.forPath(spark, dir.getCanonicalPath)
      deltaTable.delete("c2 like 'test-thin2-%'")
      val snapshotFiles_4 = dir.listFiles().filter(_.isFile).filter(_.getName.endsWith("snappy.parquet"))
        .filterNot(f => snapshotFiles_1.exists(_.getName == f.getName))
        .filterNot(f => snapshotFiles_2.exists(_.getName == f.getName))
      // snapshot is not changed before call update()
      assert(logThin.snapshot.version == 3)
      assert(logThin.snapshot.allFiles.forall { f =>
        snapshotFiles_3.exists(_.getName == f.path) })
      assert(snapshotFiles_3.length == logThin.snapshot.allFiles.size)

      // snapshot is changed after call update()
      logThin.update()
      assert(logThin.snapshot.version == 4)
      assert(logThin.snapshot.allFiles.forall { f =>
        snapshotFiles_4.exists(_.getName == f.path) })
      assert(snapshotFiles_4.length == logThin.snapshot.allFiles.size)
      assert(convertMetaData(logThin.snapshot.metadata) == log.snapshot.metadata)
      assert(convertProtocol(logThin.snapshot.protocol) == log.snapshot.protocol)

      //STEP-6: update data
      deltaTable.updateExpr("c2 like 'test-thin3-%'", Map("c2" -> "'test-thin3-update'"))
      val snapshotFiles_5 = dir.listFiles().filter(_.isFile).filter(_.getName.endsWith("snappy.parquet"))
        .filterNot(f => snapshotFiles_1.exists(_.getName == f.getName))
        .filterNot(f => snapshotFiles_2.exists(_.getName == f.getName))
        .filterNot(f => snapshotFiles_3.exists(_.getName == f.getName))
      // snapshot is not changed before call update()
      assert(logThin.snapshot.version == 4)
      assert(logThin.snapshot.allFiles.forall { f =>
        snapshotFiles_4.exists(_.getName == f.path) })
      assert(snapshotFiles_4.length == logThin.snapshot.allFiles.size)

      // snapshot is changed after call update()
      logThin.update()
      assert(logThin.snapshot.version == 5)
      assert(logThin.snapshot.allFiles.forall { f =>
        snapshotFiles_5.exists(_.getName == f.path) })
      assert(snapshotFiles_5.length == logThin.snapshot.allFiles.size)
      assert(convertMetaData(logThin.snapshot.metadata) == log.snapshot.metadata)
      assert(convertProtocol(logThin.snapshot.protocol) == log.snapshot.protocol)

      //STEP-7: compaction
      val allFilesBeforeCompaction = dir.listFiles().filter(_.isFile).filter(_.getName.endsWith("snappy.parquet"))
      spark.read
        .format("delta")
        .load(dir.getCanonicalPath)
        .repartition(2)
        .write
        .format("delta")
        .mode("overwrite")
        .save(dir.getCanonicalPath)
      val snapshotFiles_6 = dir.listFiles().filter(_.isFile).filter(_.getName.endsWith("snappy.parquet"))
        .filterNot(f => allFilesBeforeCompaction.exists(_.getName == f.getName))
      // snapshot is not changed before call update()
      assert(logThin.snapshot.version == 5)
      assert(logThin.snapshot.allFiles.forall { f =>
        snapshotFiles_5.exists(_.getName == f.path) })
      assert(snapshotFiles_5.length == logThin.snapshot.allFiles.size)

      // snapshot is changed after call update()
      logThin.update()
      assert(logThin.snapshot.version == 6)
      assert(logThin.snapshot.allFiles.forall { f =>
        snapshotFiles_6.exists(_.getName == f.path) })
      assert(snapshotFiles_6.length == logThin.snapshot.allFiles.size)
      assert(convertMetaData(logThin.snapshot.metadata) == log.snapshot.metadata)
      assert(convertProtocol(logThin.snapshot.protocol) == log.snapshot.protocol)

      //STEP-8: vacuum
      spark.sql("set spark.databricks.delta.retentionDurationCheck.enabled=false")
      deltaTable.vacuum(0.0)
      assert(logThin.snapshot.version == 6)
      assert(logThin.snapshot.allFiles.forall { f =>
        snapshotFiles_6.exists(_.getName == f.path) })
      assert(snapshotFiles_6.length == logThin.snapshot.allFiles.size)

      logThin.update()
      assert(logThin.snapshot.version == 6)
      assert(logThin.snapshot.allFiles.forall { f =>
        snapshotFiles_6.exists(_.getName == f.path) })
      assert(snapshotFiles_6.length == logThin.snapshot.allFiles.size)
      assert(convertMetaData(logThin.snapshot.metadata) == log.snapshot.metadata)
      assert(convertProtocol(logThin.snapshot.protocol) == log.snapshot.protocol)
    }
  }
}