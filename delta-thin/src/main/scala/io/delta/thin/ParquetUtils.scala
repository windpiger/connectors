package io.delta.thin

import com.github.mjakubowski84.parquet4s.ParquetReader
import io.delta.thin.actions.SingleAction
import io.delta.thin.storage.HDFSLogStore
import io.delta.thin.util.JsonUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

object ParquetUtils {

  def main(args: Array[String]): Unit = {
//    val xx = "file:///Users/songjun.sj/Desktop/testdelta/_delta_log/00000000000000000000.json"
//    val parquetIterable = ParquetReader.read[SingleAction](xx)
//
//    println("00000")

    val store = new HDFSLogStore(new Configuration())
    val paths = Seq(
      "file:///Users/songjun.sj/Desktop/testdelta/_delta_log/00000000000000000010.checkpoint.parquet")
    val pathAndFormats = paths.map(path => path -> path.split("\\.").last)
    val xx =  pathAndFormats.groupBy(_._2).flatMap { case (format, paths) =>

      paths.map(_._1).flatMap { p =>
        if (format == "json") {
          store.read(p).map { line =>
            JsonUtils.mapper.readValue[SingleAction](line)
          }
        } else if (format == "parquet") {
          ParquetReader.read[SingleAction](p).toSeq
        } else Seq.empty[SingleAction]
      }
//      if (format == "json") {
//        paths.map(_._1).flatMap { p =>
//          store.read(p).map { line =>
//            JsonUtils.mapper.readValue[SingleAction](line)
//          }
//        }
//      } else if (format == "parquet") {
//        paths.map(_._1).flatMap { p =>
//          ParquetReader.read[SingleAction](p).toSeq
//        }
//      } else Seq.empty[SingleAction]
    }

    println("0000")


  }
//  val x = new HDFSLogStore(new Configuration())
//  val y = x.read("file:///Users/songjun.sj/Desktop/testdelta/_delta_log/00000000000000000000.json")
//    .map{ line =>JsonUtils.mapper.readValue[SingleAction](line) }

}
