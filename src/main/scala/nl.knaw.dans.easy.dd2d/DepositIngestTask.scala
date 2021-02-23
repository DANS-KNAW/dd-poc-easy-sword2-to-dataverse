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
import nl.knaw.dans.easy.dd2d.OutboxSubdir.{ FAILED, OutboxSubdir, PROCESSED, REJECTED }
import nl.knaw.dans.easy.dd2d.dansbag.{ DansBagValidationResult, DansBagValidator }
import nl.knaw.dans.easy.dd2d.mapping.JsonObject
import nl.knaw.dans.lib.dataverse.DataverseInstance
import nl.knaw.dans.lib.dataverse.model.dataset.{ PrimitiveSingleValueField, UpdateType, toFieldMap }
import nl.knaw.dans.lib.error._
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import nl.knaw.dans.lib.taskqueue.Task

import java.nio.file.Path
import scala.language.postfixOps
import scala.util.{ Success, Try }
import scala.xml.Elem

/**
 * Checks one deposit and then ingests it into Dataverse.
 *
 * @param deposit  the deposit to ingest
 * @param instance the Dataverse instance to ingest in
 */
case class DepositIngestTask(deposit: Deposit,
                             dansBagValidator: DansBagValidator,
                             instance: DataverseInstance,
                             publish: Boolean = true,
                             publishAwaitUnlockMaxNumberOfRetries: Int,
                             publishAwaitUnlockMillisecondsBetweenRetries: Int,
                             narcisClassification: Elem,
                             isoToDataverseLanguage: Map[String, String],
                             outboxDir: Path) extends Task[Deposit] with DebugEnhancedLogging {
  trace(deposit, instance)

  private val mapper = new DepositToDataverseMapper(narcisClassification, isoToDataverseLanguage)
  private val bagDirPath = File(deposit.bagDir.path)

  override def run(): Try[Unit] = doRun()
    .doIfSuccess(_ => moveDepositToOutbox(PROCESSED))
    .doIfFailure {
      case _: RejectedDepositException => moveDepositToOutbox(REJECTED)
      case _ => moveDepositToOutbox(FAILED)
    }

  private def doRun(): Try[Unit] = {
    trace(())
    logger.info(s"Ingesting $deposit into Dataverse")

    for {
      validationResult <- dansBagValidator.validateBag(bagDirPath)
      _ <- rejectIfInvalid(validationResult)

      // TODO: base contact on owner of deposit
      response <- instance.admin().getSingleUser("dataverseAdmin")
      user <- response.data
      datasetContacts <- createDatasetContacts(user.displayName, user.email)
      ddm <- deposit.tryDdm
      dataverseDataset <- mapper.toDataverseDataset(ddm, datasetContacts, deposit.vaultMetadata)
      isUpdate <- deposit.isUpdate
      _ = debug(s"isUpdate? = $isUpdate")
      editor = if (isUpdate) new DatasetUpdater(deposit, dataverseDataset.datasetVersion.metadataBlocks, instance)
               else new DatasetCreator(deposit, dataverseDataset, instance)
      persistentId <- editor.performEdit()
      _ <- if (publish) publishDataset(persistentId)
           else keepOnDraft()
    } yield ()
    // TODO: delete draft if something went wrong
  }

  def moveDepositToOutbox(subDir: OutboxSubdir): Unit = {
    try {
      deposit.dir.copyToDirectory(File(outboxDir) / subDir.toString))
      deposit.dir.delete()
    } catch {
      case e: Exception => logger.info(s"Failed to move deposit: $deposit to the designated outbox : $e")
    }
  }

  private def rejectIfInvalid(validationResult: DansBagValidationResult): Try[Unit] = Try {
    if (!validationResult.isCompliant) throw RejectedDepositException(deposit,
      s"""
         |Bag was not valid according to Profile Version ${ validationResult.profileVersion }.
         |Violations:
         |${ validationResult.ruleViolations.map(_.map(formatViolation).mkString("\n")).getOrElse("") }
                      """.stripMargin)
  }

  private def formatViolation(v: (String, String)): String = v match {
    case (nr, msg) => s" - [$nr] $msg"
  }

  private def createDatasetContacts(name: String, email: String): Try[List[JsonObject]] = Try {
    List(toFieldMap(
      PrimitiveSingleValueField("datasetContactName", name),
      PrimitiveSingleValueField("datasetContactEmail", email)
    ))
  }

  private def publishDataset(persistentId: String): Try[Unit] = {
    debug("Publishing dataset")
    for {
      _ <- instance.dataset(persistentId).publish(UpdateType.major).map(_ => ())
      _ <- instance.dataset(persistentId).awaitUnlock(
        maxNumberOfRetries = publishAwaitUnlockMaxNumberOfRetries,
        waitTimeInMilliseconds = publishAwaitUnlockMillisecondsBetweenRetries)
    } yield ()
  }

  private def keepOnDraft(): Try[Unit] = {
    debug("Keeping dataset on DRAFT")
    Success(())
  }

  override def getTarget: Deposit = {
    deposit
  }

  override def toString: DepositName = {
    s"DepositIngestTask for ${ deposit }"
  }
}
