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
import nl.knaw.dans.easy.dd2d.mapping.FileElement
import nl.knaw.dans.lib.dataverse.model.file.FileMeta

import scala.util.Try
import scala.xml.Node

case class FileInfo(file: File, metadata: FileMeta)

class FilesXmlToDataverseMapper(bagDir: File) {
  def toFileInfos(node: Node, defaultRestrict: Boolean): Try[List[FileInfo]] = Try {
    (node \ "file").map(n => FileInfo(getFile(n), FileElement.toFileValueObject(n, defaultRestrict))).toList
  }


  private def getFile(node: Node): File = {
    val filePathAttr = node.attribute("filepath").flatMap(_.headOption).getOrElse { throw new RuntimeException("File node without a filepath attribute") }.text
    bagDir / filePathAttr
  }
}
