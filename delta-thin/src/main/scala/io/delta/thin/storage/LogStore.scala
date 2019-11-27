package io.delta.thin.storage

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}


/**
 * General interface for all critical file system operations required to read and write the
 * [[DeltaLog]]. The correctness of the [[DeltaLog]] is predicated on the atomicity and
 * durability guarantees of the implementation of this interface. Specifically,
 *
 * 1. Atomic visibility of files: Any file written through this store must
 *    be made visible atomically. In other words, this should not generate partial files.
 *
 * 2. Mutual exclusion: Only one writer must be able to create (or rename) a file at the final
 *    destination.
 *
 * 3. Consistent listing: Once a file has been written in a directory, all future listings for
 *    that directory must return that file.
 */
trait LogStore {

  /** Read the given `path` */
  final def read(path: String): Seq[String] = read(new Path(path))

  /** Read the given `path` */
  def read(path: Path): Seq[String]

  /**
   * Write the given `actions` to the given `path` without overwriting any existing file.
   * Implementation must throw [[java.nio.file.FileAlreadyExistsException]] exception if the file
   * already exists. Furthermore, implementation must ensure that the entire file is made
   * visible atomically, that is, it should not generate partial files.
   */
  final def write(path: String, actions: Iterator[String]): Unit = write(new Path(path), actions)

  /**
   * Write the given `actions` to the given `path` with or without overwrite as indicated.
   * Implementation must throw [[java.nio.file.FileAlreadyExistsException]] exception if the file
   * already exists and overwrite = false. Furthermore, implementation must ensure that the
   * entire file is made visible atomically, that is, it should not generate partial files.
   */
  def write(path: Path, actions: Iterator[String], overwrite: Boolean = false): Unit

  /**
   * List the paths in the same directory that are lexicographically greater or equal to
   * (UTF-8 sorting) the given `path`. The result should also be sorted by the file name.
   */
  final def listFrom(path: String): Iterator[FileStatus] = listFrom(new Path(path))

  /**
   * List the paths in the same directory that are lexicographically greater or equal to
   * (UTF-8 sorting) the given `path`. The result should also be sorted by the file name.
   */
  def listFrom(path: Path): Iterator[FileStatus]

  /** Invalidate any caching that the implementation may be using */
  def invalidateCache(): Unit

  /** Resolve the fully qualified path for the given `path`. */
  def resolvePathOnPhysicalStorage(path: Path): Path = {
    throw new UnsupportedOperationException()
  }

  /**
   * Whether a partial write is visible when writing to `path`.
   *
   * As this depends on the underlying file system implementations, we require the input of `path`
   * here in order to identify the underlying file system, even though in most cases a log store
   * only deals with one file system.
   *
   * The default value is only provided here for legacy reasons, which will be removed.
   * Any LogStore implementation should override this instead of relying on the default.
   */
  def isPartialWriteVisible(path: Path): Boolean = true
}

object LogStore extends LogStoreProvider {
  def apply(hadoopConf: Configuration): LogStore = {
    createLogStore(hadoopConf)
  }
}

trait LogStoreProvider {
  def createLogStore(hadoopConf: Configuration): LogStore = new HDFSLogStore(hadoopConf)
}
