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
package nl.knaw.dans.easy.dd2d.mapping

import java.nio.file.Paths

import nl.knaw.dans.lib.dataverse.model.dataset.DataverseFile

import scala.xml.Node

object FileElement {
  private val accessibilityToRestrict = Map(
    "KNOWN" -> "true",
    "NONE" -> "true",
    "RESTRICTED_REQUEST" -> "true",
    "ANONYMOUS" -> "false"
  )

  def toFileValueObject(node: Node, defaultRestrict: Boolean): DataverseFile = {
    val pathAttr = node.attribute("filepath").flatMap(_.headOption).getOrElse { throw new RuntimeException("File node without a filepath attribute") }.text
    if (!pathAttr.startsWith("data/")) throw new RuntimeException(s"file outside data folder: $pathAttr")
    val dirPath = Option(Paths.get(pathAttr.substring("data/".length)).getParent).map(_.toString)
    val descr = (node \ "description").headOption.map(_.text)
    val cats = (node \ "subject").map(_.text).toList
    val restr = (node \ "accessibleToRights").headOption.map(_.text).flatMap(accessibilityToRestrict.get).orElse(Some(defaultRestrict.toString))

    DataverseFile(
      directoryLabel = dirPath,
      description = descr,
      restrict = restr,
      // TODO: what do we use categories for, if anything?
      //      categories = cats
    )
  }
}
