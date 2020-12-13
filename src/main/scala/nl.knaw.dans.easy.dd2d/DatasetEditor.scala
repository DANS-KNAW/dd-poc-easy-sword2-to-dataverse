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

import java.nio.file.{ Path, Paths }

import better.files.File
import nl.knaw.dans.easy.dd2d.mapping.{ AccessRights, FileElement }
import nl.knaw.dans.lib.dataverse.DataverseInstance
import nl.knaw.dans.lib.error.TraversableTryExtensions
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.util.Try
import scala.xml.Node

/**
 * Object that edits a dataset, a new draft.
 */
abstract class DatasetEditor(deposit: Deposit, instance: DataverseInstance) extends DebugEnhancedLogging {
  type PersistendId = String

  /**
   * Performs the task.
   *
   * @return the persistentId of the dataset created or modified
   */
  def performEdit(): Try[PersistendId]

  protected def getLocalPathToFileInfo: Try[Map[Path, FileInfo]] = {
    import scala.language.postfixOps
    for {
      filesXml <- deposit.tryFilesXml
      ddm <- deposit.tryDdm
      defaultRestrict = (ddm \ "profile" \ "accessRights").headOption.forall(AccessRights toDefaultRestrict)
      files <- toFileInfos(filesXml, defaultRestrict)
    } yield files
  }

  def toFileInfos(node: Node, defaultRestrict: Boolean): Try[Map[Path, FileInfo]] = Try {
    (node \ "file").map(n => (getFilePath(n), FileInfo(getFile(n), FileElement.toFileValueObject(n, defaultRestrict)))).toMap
  }

  private def getFilePath(node: Node): Path = {
    Paths.get(node.attribute("filepath").flatMap(_.headOption).getOrElse { throw new RuntimeException("File node without a filepath attribute") }.text)
  }

  private def getFile(node: Node): File = {
    deposit.bagDir / getFilePath(node).toString
  }

  protected def uploadFilesToDataset(persistentId: String, files: List[FileInfo]): Try[Unit] = {
    import scala.language.postfixOps
    trace(persistentId)
    for {
      _ <- files
        .map(f => instance.dataset(persistentId)
          .addFile(f.file, Option(f.metadata))
          .map(_ => instance.dataset(persistentId).awaitUnlock()))
        .collectResults
    } yield ()
  }
}
