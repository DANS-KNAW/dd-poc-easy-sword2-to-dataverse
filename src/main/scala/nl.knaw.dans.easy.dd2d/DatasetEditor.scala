/**
 * Copyright (C) 2020 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
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
package nl.knaw.dans.easy.dd2d

import nl.knaw.dans.lib.dataverse.model.file.FileMeta
import nl.knaw.dans.lib.dataverse.{ DatasetApi, DataverseInstance }
import nl.knaw.dans.lib.error.TraversableTryExtensions
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.util.Try

/**
 * Object that edits a dataset, a new draft.
 */
abstract class DatasetEditor(deposit: Deposit, instance: DataverseInstance) extends DebugEnhancedLogging {
  type PersistendId = String
  protected val dataset: DatasetApi = instance.dataset(deposit.dataversePid)

  /**
   * Performs the task.
   *
   * @return the persistentId of the dataset created or modified
   */
  def performEdit(): Try[PersistendId]

  protected def addFiles(persistentId: String, files: List[FileInfo]): Try[Unit] = {
    trace(persistentId, files)
    import scala.language.postfixOps
    trace(persistentId)
    for {
      _ <- files
        .map(f => {
          debug(s"Adding file, directoryLabel = ${f.metadata.directoryLabel}, label = ${f.metadata.label}")
          instance.dataset(persistentId)
            .addFile(f.file, Option(f.metadata))
            .map(_ => instance.dataset(persistentId).awaitUnlock())
        })
        .collectResults
    } yield ()
  }

  protected def deleteFiles(databaseIds: List[DatabaseId]): Try[Unit] = {
    databaseIds.map(id => {
      debug(s"Deleting file, databaseId = $id")
      instance.sword().deleteFile(id).map(_ => dataset.awaitUnlock())
    }).collectResults.map(_ => ())
  }

  /**
   * Replace files indicated by the checksum pairs. The first member of each pair is the old SHA-1 hash and the second the new SHA-1. The old hash is used
   * to look up the databaseId of the file to be replaced, the new one is used to look up the file + metadata to place over that old file.
   *
   * @param checksumPairsToReplace            the (old, new) SHA-1 pairs
   * @param checksumToFileMetaInLatestVersion map from SHA-1 hash to FileMeta for each file in the latest dataset version
   * @param checksumToFileInfoInDeposit       map from SHA-1 hash to FileInfo for each payload file in the deposited bag
   * @return
   */
  protected def replaceFiles(checksumPairsToReplace: List[(Sha1Hash, Sha1Hash)], checksumToFileMetaInLatestVersion: Map[Sha1Hash, FileMeta], checksumToFileInfoInDeposit: Map[Sha1Hash, FileInfo]): Try[Unit] = {
    checksumPairsToReplace.map {
      case (oldChecksum, newChecksum) =>
        val fileApi = instance.file(checksumToFileMetaInLatestVersion(oldChecksum).dataFile.get.id)
        val newFileInfo = checksumToFileInfoInDeposit(newChecksum)
        debug(s"Replacing file, directoryLabel = ${newFileInfo.metadata.directoryLabel}, label = ${newFileInfo.metadata.label}")
        fileApi.replace(newFileInfo.file, newFileInfo.metadata)
        dataset.awaitUnlock()
    }.collectResults.map(_ => ())
  }
}
