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
import nl.knaw.dans.lib.dataverse.DataverseInstance
import nl.knaw.dans.lib.dataverse.model.dataset.{ CompoundField, MetadataBlock, MetadataBlocks, MetadataField, PrimitiveSingleValueField, toFieldMap }
import nl.knaw.dans.lib.dataverse.model.file.FileMeta
import nl.knaw.dans.lib.error.TraversableTryExtensions
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.collection.JavaConverters.{ asScalaSetConverter, mapAsScalaMapConverter }
import scala.util.{ Failure, Success, Try }

class DatasetUpdater(deposit: Deposit, metadataBlocks: MetadataBlocks, instance: DataverseInstance) extends DatasetEditor(deposit, instance) with DebugEnhancedLogging {
  trace(deposit)
  private val dataset = instance.dataset(deposit.dataversePid)

  override def performEdit(): Try[PersistendId] = {
    for {
      _ <- dataset.awaitUnlock()
      // TODO: owner user data, for now use dataverseAdmin
      response <- instance.admin().getSingleUser("dataverseAdmin")
      user <- response.data
      datasetContact <- createDatasetContact(user.displayName, user.email)
      _ <- dataset.updateMetadata(metadataBlocks + ("citation" -> addFieldToMetadataBlock(datasetContact, metadataBlocks("citation"))))
      _ <- dataset.awaitUnlock()
      pathToFileInfo <- getLocalPathToFileInfo
      checksumToFileInfoInDeposit <- getFilesInDeposit(pathToFileInfo)
      checksumToFileMetaInLatestVersion <- getFilesInLatestVersion

      checksumsFilesToDelete = checksumToFileMetaInLatestVersion.keySet diff checksumToFileInfoInDeposit.keySet
      _ <- logFilesToDelete(checksumsFilesToDelete, checksumToFileMetaInLatestVersion)
      _ <- deleteFiles(checksumsFilesToDelete.map(checksumToFileMetaInLatestVersion).map(_.dataFile.get.id).toList)

      checksumsFilesToAdd = checksumToFileInfoInDeposit.keySet diff checksumToFileMetaInLatestVersion.keySet
      _ <- logFilesToAdd(checksumsFilesToAdd, checksumToFileInfoInDeposit)
      _ <- uploadFilesToDataset(deposit.dataversePid, checksumsFilesToAdd.map(checksumToFileInfoInDeposit).toList)

      // TODO: Replace files with same directoryLabel, label but different checksums

    } yield deposit.dataversePid
  }




  private def logFilesToAdd(checksums: Set[Sha1Hash], checksumToFileInfo: Map[Sha1Hash, FileInfo] ): Try[Unit] = Try {
    if (logger.underlying.isDebugEnabled) debugFiles("Files to add", checksums.map(checksumToFileInfo).map(_.metadata).toList)
    else Success(())
  }

  private def logFilesToDelete(checksums: Set[Sha1Hash], checksumToFileMeta: Map[Sha1Hash, FileMeta] ): Try[Unit] = Try {
    if (logger.underlying.isDebugEnabled) debugFiles("Files to delete", checksums.map(checksumToFileMeta).toList)
    else Success(())
  }

  private def debugFiles(prefix: String, files: List[FileMeta]): Unit = {
    debug(s"$prefix: ${ files.map(f => f.directoryLabel.getOrElse("/") + f.label.getOrElse("")).mkString(", ") }")
  }

  private def deleteFiles(databaseIds: List[DatabaseId]): Try[Unit] = {
    databaseIds.map(id => instance.sword().deleteFile(id).map(_ => dataset.awaitUnlock())).collectResults.map(_ => ())
  }

  private def createDatasetContact(name: String, email: String): Try[CompoundField] = Try {
    CompoundField(
      typeName = "datasetContact",
      value =
        List(toFieldMap(
          PrimitiveSingleValueField("datasetContactName", name),
          PrimitiveSingleValueField("datasetContactEmail", email)
        ))
    )
  }

  private def addFieldToMetadataBlock(field: MetadataField, block: MetadataBlock): MetadataBlock = {
    block.copy(fields = field :: block.fields)
  }

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

  private def getFilesInDeposit(fileInfos: Map[Path, FileInfo]): Try[Map[Sha1Hash, FileInfo]] = {
    for {
      bag <- deposit.tryBag
      optSha1Manifest = bag.getPayLoadManifests.asScala.find(_.getAlgorithm == StandardSupportedAlgorithms.SHA1)
      _ = if (optSha1Manifest.isEmpty) throw new IllegalArgumentException("Deposit bag does not have SHA-1 payload manifest")
      sha1ToFilePath = optSha1Manifest.get.getFileToChecksumMap.asScala.map { case (p, c) => (c, deposit.bagDir.path relativize p) }
      sha1ToFileInfo = sha1ToFilePath.map { case (sha1, path) => (sha1 -> fileInfos(path)) }.toMap
    } yield sha1ToFileInfo
  }
}
