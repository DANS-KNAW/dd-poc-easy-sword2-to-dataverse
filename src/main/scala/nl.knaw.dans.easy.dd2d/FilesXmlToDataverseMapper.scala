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

import java.nio.file.Paths

import better.files.File
import nl.knaw.dans.easy.dd2d.dataverse.json.{ FileInformation, FileMetadata }

import scala.collection.mutable.ListBuffer
import scala.xml.Node

class FilesXmlToDataverseMapper {
  private val projectRootToFilePathAttribute = File("data/inbox/valid-easy-submitted/example-bag-medium/")

  def extractFileInfoFromFilesXml(filesXml: Node): Seq[FileInformation] = {
    val files = new ListBuffer[FileInformation]
    (filesXml \\ "file").filter(_.nonEmpty).foreach(n => {
      val description = (n \ "description").headOption.map(_.text)
      val directoryLabel = (n \ "@filepath").headOption.map(_.text)
      val restrict = Some("false")
      files += FileInformation(File(projectRootToFilePathAttribute + directoryLabel.get),
        FileMetadata(description, getDirPath(directoryLabel), restrict))
    })
    files.toList
  }

  def getDirPath(fullPath: Option[String]): Option[String] = {
    fullPath.map(p => Paths.get(p).getParent.toString)
  }
}
