package io.delta.thin

import io.delta.thin.util.JsonUtils
import org.apache.hadoop.fs.Path

class Snapshot(
    val path: Path,
    val version: Long,
    files: Seq[Path],
    val deltaLog: DeltaLog,
    val timestamp: Long,
    val lineageLength: Int = 1) {

//
//  /**
//    * Load the transaction logs from paths. The files here may have different file formats and the
//    * file format can be extracted from the file extensions.
//    *
//    * Here we are reading the transaction log, and we need to bypass the ACL checks
//    * for SELECT any file permissions.
//    */
//  private def load(paths: Seq[Path]): Set[SingleAction] = {
//    val pathAndFormats = paths.map(_.toString).map(path => path -> path.split("\\.").last)
//    pathAndFormats.groupBy(_._2).map { case (format, paths) =>
//      if (format == "json") {
//        paths.map(_._1).flatMap { p =>
//          deltaLog.store.read(p).map { line =>
//            JsonUtils.mapper.readValue[SingleAction](line)
//          }
//        }
//      } else if (format == "parquet") {
//        val parquetIterable = ParquetReader.read[User](path)
//
//      }
//    }.reduceOption(_.union(_)).getOrElse(emptyActions)
//  }
//
//  // Reconstruct the state by applying deltas in order to the checkpoint.
//  // We partition by path as it is likely the bulk of the data is add/remove.
//  // Non-path based actions will be collocated to a single partition.
//  private val stateReconstruction = {
//    val implicits = spark.implicits
//    import implicits._
//
//    val numPartitions = spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_SNAPSHOT_PARTITIONS)
//
//    val checkpointData = previousSnapshot.getOrElse(emptyActions)
//    val deltaData = load(files)
//    val allActions = checkpointData.union(deltaData)
//    val time = minFileRetentionTimestamp
//    val hadoopConf = new SerializableConfiguration(spark.sessionState.newHadoopConf())
//    val logPath = path.toUri // for serializability
//
//    allActions.as[SingleAction]
//      .mapPartitions { actions =>
//        val hdpConf = hadoopConf.value
//        actions.flatMap {
//          _.unwrap match {
//            case add: AddFile => Some(add.copy(path = canonicalizePath(add.path, hdpConf)).wrap)
//            case rm: RemoveFile => Some(rm.copy(path = canonicalizePath(rm.path, hdpConf)).wrap)
//            case other if other == null => None
//            case other => Some(other.wrap)
//          }
//        }
//      }
//      .withColumn("file", assertLogBelongsToTable(logPath)(input_file_name()))
//      .repartition(numPartitions, coalesce($"add.path", $"remove.path"))
//      .sortWithinPartitions("file")
//      .as[SingleAction]
//      .mapPartitions { iter =>
//        val state = new InMemoryLogReplay(time)
//        state.append(0, iter.map(_.unwrap))
//        state.checkpoint.map(_.wrap)
//      }
//  }
}
