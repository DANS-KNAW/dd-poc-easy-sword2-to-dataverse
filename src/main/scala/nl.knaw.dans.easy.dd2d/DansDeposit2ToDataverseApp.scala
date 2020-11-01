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

import java.io.PrintStream

import better.files.File
import nl.knaw.dans.easy.dd2d.dansbag.DansBagValidator
import nl.knaw.dans.easy.dd2d.dataverse.DataverseInstance
import nl.knaw.dans.easy.dd2d.queue.{ ActiveTaskQueue, PassiveTaskQueue }
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.json4s.{ DefaultFormats, Formats }

import scala.util.Try

class DansDeposit2ToDataverseApp(configuration: Configuration) extends DebugEnhancedLogging {
  private implicit val resultOutput: PrintStream = Console.out
  private val dataverse = new DataverseInstance(configuration.dataverse)
  private val dansBagValidator = new DansBagValidator(configuration.validatorServiceUrl)
  private val inboxWatcher = new InboxWatcher(new Inbox(configuration.inboxDir, dansBagValidator, dataverse, configuration.autoPublish))

  def checkPreconditions(): Try[Unit] = {
    for {
      _ <- dansBagValidator.checkConnection()
      _ <- dataverse.checkConnection()
    } yield ()
  }

  def importSingleDeposit(deposit: File, autoPublish: Boolean): Try[Unit] = {
    new SingleDepositProcessor(deposit, dansBagValidator, dataverse, autoPublish).process()
  }

  def importDeposits(inbox: File, autoPublish: Boolean): Try[Unit] = Try {
    new InboxProcessor(new Inbox(inbox, dansBagValidator, dataverse, autoPublish)).process()
  }

  def start(): Try[Unit] = Try {
    inboxWatcher.start()
  }

  def stop(): Try[Unit] = Try {
    inboxWatcher.stop()
  }
}
