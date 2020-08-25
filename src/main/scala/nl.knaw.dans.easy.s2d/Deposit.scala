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

import better.files.File
import gov.loc.repository.bagit.domain.Bag
import gov.loc.repository.bagit.reader.BagReader
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.util.{ Failure, Try }
import scala.xml.{ Node, Utility, XML }

/**
 * Represents a deposit directory and provides access to the files and metadata in it.
 *
 * @param dir the deposit directory
 */
case class Deposit(dir: File) extends DebugEnhancedLogging {
  trace(dir)
  val bagDir: File = {
    val dirs = dir.children.filter(_.isDirectory).toList
    debug(s"dirs = $dirs")
    if (dirs.size != 1) throw new IllegalArgumentException(s"$dir is not a valid deposit. Found ${dirs.size} subdirectories instead of 1")
    dirs.head
  }
  debug(s"bagDir = $bagDir")

  lazy val triedBag: Try[Bag] = Try { new BagReader().read(bagDir.path) }
  lazy val triedNode: Try[Node] = load(bagDir / "metadata" / "dataset.xml")
  lazy val triedFilesXml: Try[Node] = load(bagDir / "metadata" / "files.xml")

  private def load(path: File) = Try {
    Utility.trim {
      XML.loadFile((bagDir / path.toString).toJava)
    }
  }.recoverWith {
    case t: Throwable => Failure(new IllegalArgumentException(s"Unparseable XML: ${ t.getMessage }"))
  }

  override def toString: String = s"Deposit at $dir"
}
