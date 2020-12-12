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

import gov.loc.repository.bagit.hash.{ StandardSupportedAlgorithms, SupportedAlgorithm }
import nl.knaw.dans.lib.dataverse.DataverseInstance
import nl.knaw.dans.lib.dataverse.model.dataset.{ CompoundField, MetadataBlock, MetadataBlocks, MetadataField, PrimitiveSingleValueField, toFieldMap }
import nl.knaw.dans.lib.error.TraversableTryExtensions
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.collection.JavaConverters.{ asScalaSetConverter, mapAsScalaMapConverter }
import scala.util.Try

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
      fileInfos <- getFileInfos
      sha1ToFileInfos <- getFilesInDeposit(fileInfos)
      sha1ToDatabaseId <- getFilesInLatestVersion
      filesToDelete = sha1ToDatabaseId.keySet diff sha1ToFileInfos.keySet
      _ = if(logger.underlying.isDebugEnabled) debug(s"Files to delete: $filesToDelete")
      _ <- deleteFiles(filesToDelete.map(sha1ToDatabaseId).toList)
      _ <- dataset.awaitUnlock()
      filesToAdd = sha1ToFileInfos.keySet diff sha1ToDatabaseId.keySet
      _ = if (logger.underlying.isDebugEnabled) debug(s"Files to add: $filesToAdd")
      _ <- uploadFilesToDataset(deposit.dataversePid, filesToAdd.map(sha1ToFileInfos).toList)
      _ <- dataset.awaitUnlock()
      // TODO: update metadata
    } yield deposit.dataversePid
  }

  private def deleteFiles(databaseIds: List[Int]): Try[Unit] = {
    databaseIds.map(instance.sword().deleteFile).collectResults.map(_ => ())
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

  private def getFilesInLatestVersion: Try[Map[String, Int]] = {
    for {
      response <- dataset.listFiles()
      files <- response.data
      optDataFiles = files.map(_.dataFile)
      _ = if (!optDataFiles.forall(_.isDefined)) throw new IllegalStateException("Could not retrieve checksum for all files")
      dataFiles = optDataFiles.map(_.get)
      _ = if (!dataFiles.forall(_.checksum.`type` == "SHA-1")) throw new IllegalStateException("Not all checksums are of type SHA-1")
      checksumToId = dataFiles.map(df => (df.checksum.value, df.id)).toMap
    } yield checksumToId
  }

  private def getFilesInDeposit(fileInfos: Map[String, FileInfo]): Try[Map[String, FileInfo]] = {
    for {
      bag <- deposit.tryBag
      optSha1Manifest = bag.getPayLoadManifests.asScala.find(_.getAlgorithm == StandardSupportedAlgorithms.SHA1)
      _ = if(optSha1Manifest.isEmpty) throw new IllegalArgumentException("Deposit bag does not have SHA-1 payload manifest")
      sha1ToFilePath = optSha1Manifest.get.getFileToChecksumMap.asScala.map { case (p, c) => (c, deposit.bagDir.path relativize p) }
      sha1ToFileInfo = sha1ToFilePath.map { case (sha1, path) => (sha1 -> fileInfos(path.toString)) }.toMap
    } yield sha1ToFileInfo
  }
}
