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

import java.util.concurrent.Executors

import better.files.{ File, FileMonitor }
import nl.knaw.dans.easy.dd2d.dataverse.DataverseInstance
import nl.knaw.dans.easy.dd2d.queue.ActiveTaskQueue
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.json4s.{ DefaultFormats, Formats }

import scala.concurrent.ExecutionContext

/**
 * Object that monitors `inbox` directory for new sub-directories to appear. For each new subdirectory
 * it schedules an DepositIngestTask on an ActiveTaskQueue. This task ingests the deposit to the
 * the Dataverse instance represented by `dataverse`.
 *
 * @param inbox     the inbox directory to monitor
 */
class InboxWatcher(inbox: Inbox) extends DebugEnhancedLogging {
  implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())
  private val ingestTasks: ActiveTaskQueue = new ActiveTaskQueue()
  private val monitor = inbox.createFileMonitor(ingestTasks)

  def start(): Unit = {
    trace(())
    logger.info("Enqueuing deposits found in inbox...")
    inbox.enqueue(ingestTasks)
    logger.info("Start processing deposits...")
    ingestTasks.start()
    logger.info("Starting inbox monitor...")
    monitor.start()
  }

  def stop(): Unit = {
    trace(())
    logger.info("Sending stop item to queue...")
    ingestTasks.stop()
  }
}
