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

import better.files.File
import nl.knaw.dans.easy.dd2d.dansbag.DansBagValidator
import nl.knaw.dans.easy.dd2d.mapping.AccessRights
import nl.knaw.dans.lib.dataverse.model.dataset.{ CompoundField, DatasetCreationResult, PrimitiveSingleValueField, UpdateType, toFieldMap }
import nl.knaw.dans.lib.dataverse.{ DataverseInstance, DataverseResponse }
import nl.knaw.dans.lib.error._
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import nl.knaw.dans.lib.taskqueue.Task
import org.json4s.Formats

import scala.language.postfixOps
import scala.util.{ Success, Try }

/**
 * Checks one deposit and then ingests it into Dataverse.
 *
 * @param deposit     the deposit to ingest
 * @param instance    the Dataverse instance to ingest in
 * @param jsonFormats implicit necessary for pretty-printing JSON
 */
case class DepositIngestTask(deposit: Deposit,
                             dansBagValidator: DansBagValidator,
                             instance: DataverseInstance,
                             publish: Boolean = true,
                             publishAwaitUnlockMaxNumberOfRetries: Int,
                             publishAwaitUnlockMillisecondsBetweenRetries: Int)(implicit jsonFormats: Formats) extends Task[Deposit] with DebugEnhancedLogging {
  trace(deposit, instance)

  private val mapper = new DepositToDataverseMapper()
  private val bagDirPath = File(deposit.bagDir.path)

  override def run(): Try[Unit] = {
    trace(())
    logger.info(s"Ingesting $deposit into Dataverse")

    for {
      validationResult <- dansBagValidator.validateBag(bagDirPath)
      _ <- Try {
        if (!validationResult.isCompliant) throw RejectedDepositException(deposit,
          s"""
             |Bag was not valid according to Profile Version ${ validationResult.profileVersion }.
             |Violations:
             |${ validationResult.ruleViolations.map(_.map(formatViolation).mkString("\n")).getOrElse("") }
                      """.stripMargin)
      }
      // TODO: base contact on owner of deposit
      response <- instance.admin().getSingleUser("dataverseAdmin")
      user <- response.data
      datasetContact <- createDatasetContact(user.displayName, user.email)
      ddm <- deposit.tryDdm
      dataverseDataset <- mapper.toDataverseDataset(ddm, datasetContact, deposit.vaultMetadata)
      isUpdate <- deposit.isUpdate
      editor = if (isUpdate) new DatasetUpdater(deposit, dataverseDataset.datasetVersion.metadataBlocks, instance)
               else new DatasetCreator(deposit, dataverseDataset, instance)
      persistentId <- editor.performEdit()
      _ <- if (publish) {
        debug("Publishing dataset")
        publishDataset(persistentId)
      }
           else {
             debug("Keeping dataset on DRAFT")
             Success(())
           }
      _ <- instance.dataset(persistentId).awaitUnlock()
      // TODO: check that dataset is indeed now published
    } yield ()
    // TODO: delete draft if something went wrong
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

  private def getPersistentId(response: DataverseResponse[DatasetCreationResult]): Try[String] = {
    response.data.map(_.persistentId)
  }

  private def formatViolation(v: (String, String)): String = v match {
    case (nr, msg) => s" - [$nr] $msg"
  }

  private def publishDataset(datasetId: String): Try[Unit] = {
    instance.dataset(datasetId).publish(UpdateType.major).map(_ => ())
  }

  override def getTarget: Deposit = {
    deposit
  }

  override def toString: DepositName = {
    s"DepositIngestTask for ${deposit}"
  }
}
