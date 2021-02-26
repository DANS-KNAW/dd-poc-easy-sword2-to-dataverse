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
import nl.knaw.dans.lib.dataverse.DataverseInstance
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import nl.knaw.dans.lib.taskqueue.InboxWatcher

import java.io.PrintStream
import scala.util.Try

class DansDeposit2ToDataverseApp(configuration: Configuration) extends DebugEnhancedLogging {
  private implicit val resultOutput: PrintStream = Console.out
  private val dataverse = new DataverseInstance(configuration.dataverse)
  private val dansBagValidator = new DansBagValidator(
    serviceUri = configuration.validatorServiceUrl,
    connTimeoutMs = configuration.validatorConnectionTimeoutMs,
    readTimeoutMs = configuration.validatorReadTimeoutMs)
  private val inboxWatcher = {
    new InboxWatcher(new Inbox(configuration.inboxDir,
      getActiveMetadataBlocks.get,
      dansBagValidator,
      dataverse,
      configuration.autoPublish,
      configuration.publishAwaitUnlockMaxNumberOfRetries,
      configuration.publishAwaitUnlockMillisecondsBetweenRetries,
      configuration.narcisClassification,
      configuration.isoToDataverseLanguage,
      configuration.outboxDir))
  }

  def checkPreconditions(): Try[Unit] = {
    for {
      _ <- dansBagValidator.checkConnection()
      _ <- dataverse.checkConnection()
    } yield ()
  }

  def importSingleDeposit(deposit: File, autoPublish: Boolean, outboxDir: File): Try[Unit] = {
    for {
      _ <- initOutboxDirs(outboxDir, requireAbsenceOfResults = false)
      - <- mustNotExist(OutboxSubdir.values.map(_.toString).map(subdir => outboxDir / subdir / deposit.name).toList)
      _ <- new SingleDepositProcessor(deposit,
        getActiveMetadataBlocks.get,
        dansBagValidator,
        dataverse,
        autoPublish,
        configuration.publishAwaitUnlockMaxNumberOfRetries,
        configuration.publishAwaitUnlockMillisecondsBetweenRetries,
        configuration.narcisClassification,
        configuration.isoToDataverseLanguage,
        outboxDir).process()
    } yield ()
  }

  def importDeposits(inbox: File, autoPublish: Boolean, outboxDir: File): Try[Unit] = {
    for {
      _ <- initOutboxDirs(outboxDir)
      _ <- new InboxProcessor(new Inbox(inbox,
        getActiveMetadataBlocks.get,
        dansBagValidator,
        dataverse,
        autoPublish,
        configuration.publishAwaitUnlockMaxNumberOfRetries,
        configuration.publishAwaitUnlockMillisecondsBetweenRetries,
        configuration.narcisClassification,
        configuration.isoToDataverseLanguage,
        outboxDir)).process()
    } yield ()
  }

  def start(): Try[Unit] = {
    inboxWatcher.start(Some(new DepositSorter()))
  }

  def stop(): Try[Unit] = {
    inboxWatcher.stop()
  }

  private def initOutboxDirs(outboxDir: File, requireAbsenceOfResults: Boolean = true): Try[Unit] = Try {
    trace(outboxDir)
    val subDirs = List(outboxDir / OutboxSubdir.PROCESSED.toString,
      outboxDir / OutboxSubdir.FAILED.toString,
      outboxDir / OutboxSubdir.REJECTED.toString)

    mustBeDirectory(outboxDir)
    if (requireAbsenceOfResults) mustBeEmptyDirectories(subDirs)
    outboxDir.createDirectoryIfNotExists(createParents = true)
    subDirs.foreach(_.createDirectories())
  }

  private def mustBeEmptyDirectories(dirs: List[File]): Try[Unit] = Try {
    dirs.find(d => d.isDirectory && d.nonEmpty).map(d => throw ExistingResultsInOutboxException(d))
  }

  private def mustBeDirectory(d: File): Try[Unit] = Try {
    if (d.isRegularFile) throw OutboxDirIsRegularFileException(d)
  }

  private def mustNotExist(fs: List[File]): Try[Unit] = Try {
    fs.find(_.exists).map(d => throw ExistingResultsInOutboxException(d))
  }

  private def getActiveMetadataBlocks: Try[List[String]] = {
    for {
      result <- dataverse.dataverse("root").listMetadataBocks()
      blocks <- result.data
    } yield blocks.map(_.name)
  }
}
