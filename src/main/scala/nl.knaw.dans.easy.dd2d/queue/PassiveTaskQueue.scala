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
package nl.knaw.dans.easy.dd2d.queue

import java.util
import java.util.concurrent.LinkedBlockingDeque

import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.collection.mutable.ListBuffer

class PassiveTaskQueue() extends DebugEnhancedLogging {
  private val tasks = new ListBuffer[Task]

  /**
   * Adds a new task to the queue.
   *
   * @param t the task to add
   */
  def add(t: Task): Unit = {
    trace(t)
    tasks += t
    debug("Task added to queue")
  }

  /**
   * Process items on the queue
   */
  def process(): Unit = {
    logger.info("Processing thread ready for running tasks")
    tasks.map {
      t => t.run().recover {
        case e: Throwable => logger.warn(s"Task $t failed", e);
      }
    }
    logger.info("Finished processing tasks.")
  }
}
