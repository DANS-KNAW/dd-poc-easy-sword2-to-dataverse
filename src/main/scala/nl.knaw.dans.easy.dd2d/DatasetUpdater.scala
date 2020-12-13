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

import java.nio.file.Path

import gov.loc.repository.bagit.hash.StandardSupportedAlgorithms
import nl.knaw.dans.easy.dd2d.mapping.JsonObject
import nl.knaw.dans.lib.dataverse.DataverseInstance
import nl.knaw.dans.lib.dataverse.model.dataset.{ MetadataBlock, MetadataBlocks, MetadataField, PrimitiveSingleValueField, toFieldMap }
import nl.knaw.dans.lib.dataverse.model.file.FileMeta
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.collection.JavaConverters.{ asScalaSetConverter, mapAsScalaMapConverter }
import scala.util.{ Failure, Success, Try }

class DatasetUpdater(deposit: Deposit, metadataBlocks: MetadataBlocks, instance: DataverseInstance) extends DatasetEditor(deposit, instance) with DebugEnhancedLogging {
  trace(deposit)

  override def performEdit(): Try[PersistendId] = {
    for {
      pathToFileInfo <- deposit.getPathToFileInfo
      checksumToFileInfoInDeposit <- getFilesInDeposit(pathToFileInfo)
      checksumToFileMetaInLatestVersion <- getFilesInLatestVersion

      // (old, new) checksum
      checksumPairsToReplace <- getFilesToReplace(checksumToFileInfoInDeposit, checksumToFileMetaInLatestVersion)
      _ <- logFilesToReplace(checksumPairsToReplace.map(_._1), checksumToFileMetaInLatestVersion)
      _ <- replaceFiles(checksumPairsToReplace, checksumToFileMetaInLatestVersion, checksumToFileInfoInDeposit)

      checksumsFilesToDelete = (checksumToFileMetaInLatestVersion.keySet diff checksumPairsToReplace.map(_._1).toSet) diff checksumToFileInfoInDeposit.keySet
      _ <- logFilesToDelete(checksumsFilesToDelete.toList, checksumToFileMetaInLatestVersion)
      _ <- deleteFiles(checksumsFilesToDelete.map(checksumToFileMetaInLatestVersion).map(_.dataFile.get.id).toList)

      checksumsFilesToAdd = (checksumToFileInfoInDeposit.keySet diff checksumPairsToReplace.map(_._2).toSet) diff checksumToFileMetaInLatestVersion.keySet
      _ <- logFilesToAdd(checksumsFilesToAdd.toList, checksumToFileInfoInDeposit)
      _ <- addFiles(deposit.dataversePid, checksumsFilesToAdd.map(checksumToFileInfoInDeposit).toList)
    } yield deposit.dataversePid
  }

  private def createDatasetContact(name: String, email: String): Try[JsonObject] = Try {
    toFieldMap(
      PrimitiveSingleValueField("datasetContactName", name),
      PrimitiveSingleValueField("datasetContactEmail", email)
    )
  }

  private def addFieldToMetadataBlock(field: MetadataField, block: MetadataBlock): MetadataBlock = {
    block.copy(fields = field :: block.fields)
  }

  /**
   * Creates a map from SHA-1 hash to FileInfo for the files in the payload of the deposited bag.
   *
   * @param pathToFileInfo map from bag-local path to FileInfo
   * @return
   */
  private def getFilesInDeposit(pathToFileInfo: Map[Path, FileInfo]): Try[Map[Sha1Hash, FileInfo]] = {
    for {
      bag <- deposit.tryBag
      optSha1Manifest = bag.getPayLoadManifests.asScala.find(_.getAlgorithm == StandardSupportedAlgorithms.SHA1)
      _ = if (optSha1Manifest.isEmpty) throw new IllegalArgumentException("Deposit bag does not have SHA-1 payload manifest")
      sha1ToFilePath = optSha1Manifest.get.getFileToChecksumMap.asScala.map { case (p, c) => (c, deposit.bagDir.path relativize p) }
      sha1ToFileInfo = sha1ToFilePath.map { case (sha1, path) => (sha1 -> pathToFileInfo(path)) }.toMap
    } yield sha1ToFileInfo
  }

  /**
   * Creates a map from SHA-1 hash to FileMeta for all the files in the latest version of the dataset.
   *
   * @return
   */
  private def getFilesInLatestVersion: Try[Map[Sha1Hash, FileMeta]] = {
    for {
      response <- dataset.listFiles()
      files <- response.data
      _ <- validateFileMetadas(files)
      checksumToFileMeta = files.map(f => (f.dataFile.get.checksum.value, f)).toMap
    } yield checksumToFileMeta
  }

  private def validateFileMetadas(files: List[FileMeta]): Try[Unit] = {
    if (files.map(_.dataFile).exists(_.isEmpty)) Failure(new IllegalArgumentException("Found file metadata without dataFile element"))
    else if (files.map(_.dataFile.get).exists(_.checksum.`type` != "SHA-1")) Failure(new IllegalArgumentException("Not all file checksums are of type SHA-1"))
         else Success(())
  }

  /**
   * Calculates which files have to be replaced and returns a list of SHA-1 has pairs, the first of which is the old hash and the second the hash of the replacement.
   * Files which have the same (directoryLabel, label) pair in the deposit and in the latest version of the dataset are marked as files to be replaced.
   *
   * @param checksumToFileInfoInDeposit       map from SHA-1 hash to FileInfo for each payload file in the deposited bag
   * @param checksumToFileMetaInLatestVersion map from SHA-1 hash to FileMeta for each file in the latest version of the dataset
   * @return
   */
  private def getFilesToReplace(checksumToFileInfoInDeposit: Map[Sha1Hash, FileInfo], checksumToFileMetaInLatestVersion: Map[Sha1Hash, FileMeta]): Try[List[(Sha1Hash, Sha1Hash)]] = {
    for {
      labelPairToChecksumDeposit <- Try { checksumToFileInfoInDeposit.map { case (c, fi) => ((fi.metadata.directoryLabel, fi.metadata.label) -> c) } }
      labelPairToChecksumLatestVersion <- Try { checksumToFileMetaInLatestVersion.map { case (c, m) => ((m.directoryLabel, m.label) -> c) } }
      intersection = labelPairToChecksumDeposit.keySet intersect labelPairToChecksumLatestVersion.keySet
      checksumsDiffer = intersection.filter(p => labelPairToChecksumDeposit(p) != labelPairToChecksumLatestVersion(p))
      toBeReplaced = checksumsDiffer.map(p => (labelPairToChecksumLatestVersion(p), labelPairToChecksumDeposit(p))).toList
    } yield toBeReplaced
  }

  /*
   * Utility function for logging.
   */

  private def logFilesToReplace(checksums: List[Sha1Hash], checksumToFileMeta: Map[Sha1Hash, FileMeta]): Try[Unit] = Try {
    if (logger.underlying.isDebugEnabled) debugFiles("Files to replace", checksums.map(checksumToFileMeta))
    else Success(())
  }

  private def logFilesToAdd(checksums: List[Sha1Hash], checksumToFileInfo: Map[Sha1Hash, FileInfo]): Try[Unit] = Try {
    if (logger.underlying.isDebugEnabled) debugFiles("Files to add", checksums.map(checksumToFileInfo).map(_.metadata).toList)
    else Success(())
  }

  private def logFilesToDelete(checksums: List[Sha1Hash], checksumToFileMeta: Map[Sha1Hash, FileMeta]): Try[Unit] = Try {
    if (logger.underlying.isDebugEnabled) debugFiles("Files to delete", checksums.map(checksumToFileMeta).toList)
    else Success(())
  }

  private def debugFiles(prefix: String, files: List[FileMeta]): Unit = {
    debug(s"$prefix: ${ files.map(f => f.directoryLabel.getOrElse("/") + f.label.getOrElse("")).mkString(", ") }")
  }
}
