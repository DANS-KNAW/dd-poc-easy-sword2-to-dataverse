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

import nl.knaw.dans.lib.dataverse.DataverseInstance
import nl.knaw.dans.lib.dataverse.model.dataset.{ CompoundField, MetadataBlock, MetadataBlocks, MetadataField, PrimitiveSingleValueField, toFieldMap }
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.util.Try

class DatasetUpdater(deposit: Deposit, metadataBlocks: MetadataBlocks, instance: DataverseInstance) extends DatasetWorker with DebugEnhancedLogging {
  trace(deposit)

  override def performTask(): Try[PersistendId] = {
    for {
      persistentId <- Try { deposit.dataversePid } // TODO: generate error if pid of one of components is not found
      // TODO: owner user data, for now use dataverseAdmin
      response <- instance.admin().getSingleUser("dataverseAdmin")
      user <- response.data
      datasetContact <- createDatasetContact(user.displayName, user.email)
      _ <- instance
        .dataset(persistentId)
        .updateMetadata(metadataBlocks + ("citation" -> addFieldToMetadataBlock(datasetContact, metadataBlocks("citation"))))
    } yield persistentId
  }

  private def createDatasetContact(name: String, email: String): Try[CompoundField] = Try {
    CompoundField(
      typeName = "datasetContact",
      value =
        toFieldMap(
          PrimitiveSingleValueField("datasetContactName", name),
          PrimitiveSingleValueField("datasetContactEmail", email)
        )
    )
  }

  private def addFieldToMetadataBlock(field: MetadataField, block: MetadataBlock): MetadataBlock = {
    block.copy(fields = field :: block.fields)
  }
}
