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
import nl.knaw.dans.easy.dd2d.dataverse.DataverseInstance
import nl.knaw.dans.easy.dd2d.queue.PassiveTaskQueue
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.json4s.Formats

import scala.util.Try

class InboxProcessor(dataverse: DataverseInstance)(implicit jsonFormats: Formats)  extends DebugEnhancedLogging {

  def process(inbox: File): Try[Unit] = Try {
    trace(())
    val ingestTasks = new PassiveTaskQueue()
    val dirs = DepositsDir(inbox).list
    logger.info(s"Queueing ${ dirs.size } directories...")
    dirs.foreach {
      d => {
        debug(s"Adding $d")
        ingestTasks.add(DepositIngestTask(Deposit(d), dataverse))
      }
    }
    logger.info("Processing queue...")
    ingestTasks.process()
  }
}
