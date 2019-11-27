/*
 * Copyright 2019 Databricks, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.thin.actions

import java.net.URI

/**
 * Replays a history of action, resolving them to produce the current state
 * of the table. The protocol for resolution is as follows:
 *  - The most recent [[AddFile]] and accompanying metadata for any `path` wins.
 *  - [[RemoveFile]] deletes a corresponding [[AddFile]] and is retained as a
 *    tombstone until `minFileRetentionTimestamp` has passed.
 *  - The most recent version for any `appId` in a [[SetTransaction]] wins.
 *  - The most recent [[Metadata]] wins.
 *  - The most recent [[Protocol]] version wins.
 *  - For each path, this class should always output only one [[FileAction]] (either [[AddFile]] or
 *    [[RemoveFile]])
 *
 * This class is not thread safe.
 */
class InMemoryLogReplay {
  var currentProtocolVersion: Protocol = null
  var currentVersion: Long = -1
  var currentMetaData: Metadata = null
  val activeFiles = new scala.collection.mutable.HashMap[URI, AddFile]()

  def append(version: Long, action: Action): Unit = {
//    assert(currentVersion == -1 || version == currentVersion + 1,
//      s"Attempted to replay version $version, but state is at $currentVersion")
    currentVersion = version
    action match {
      case a: Metadata =>
        currentMetaData = a
      case a: Protocol =>
        currentProtocolVersion = a
      case add: AddFile =>
        activeFiles(add.pathAsUri) = add.copy(dataChange = false)
      case remove: RemoveFile =>
        activeFiles.remove(remove.pathAsUri)
      case a: SetTransaction => // do nothing
      case ci: CommitInfo => // do nothing
      case null => // Some crazy future feature. Ignore
    }
  }

  /** Returns the current state of the Table as an iterator of actions. */
  def checkpoint: Iterator[Action] = {
    Option(currentProtocolVersion).toIterator ++
    Option(currentMetaData).toIterator ++
    activeFiles.values.toSeq.sortBy(_.path).iterator
  }
}
