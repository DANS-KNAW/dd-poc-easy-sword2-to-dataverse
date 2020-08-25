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
package nl.knaw.dans.easy.s2d

import java.io.PrintStream

import nl.knaw.dans.easy.s2d.dataverse.DataverseInstance

import scala.util.Try

class EasySword2ToDataverseApp(configuration: Configuration) {
  private implicit val resultOutput: PrintStream = Console.out

  private val inboxMonitor = new InboxMonitor(configuration.inboxDir, DataverseInstance(configuration.dataverse))

  def start(): Try[Unit] = Try {
    inboxMonitor.start()
  }

  def stop(): Try[Unit] = Try {
    inboxMonitor.stop()
  }
}
