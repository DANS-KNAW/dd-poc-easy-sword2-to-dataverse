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

import nl.knaw.dans.easy.dd2d.dataverse.json.{ FieldMap, JsonObject }
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.xml.Node

object Type extends BlockContentTypeAndFileFormat with DebugEnhancedLogging {
  //todo what values are allowed in ingest?
  val dcmiTypes = Map(
    "Collection" -> "Collection",
    "Dataset" -> "Dataset",
    "Event" -> "Event",
    "Image" -> "Image",
    "Interactive resource" -> "Interactive resource",
    "Moving image" -> "Moving image",
    "Physical object" -> "Physical object",
    "Service" -> "Service",
    "Software" -> "Software",
    "Sound" -> "Sound",
    "Still image" -> "Still image",
    "Text" -> "Text"
  )

  def toContentTypeAndFileFormatBlockType(node: Node): Option[JsonObject] = {
    val contentType = getContentType(node)
    contentType match {
      case "" =>
        logger.error(s"Invalid controlled vocabulary term for 'Content Type': '$contentType'")
        None
      case _ =>
        val m = FieldMap()
        m.addPrimitiveField(CONTENT_TYPE_CV_VALUE, contentType)
        m.addPrimitiveField(CONTENT_TYPE_CV_VOCABULARY, DCMI_TYPE)
        m.addPrimitiveField(CONTENT_TYPE_CV_VOCABULARY_URL, DCMI_TYPE_BASE_URL + contentType)
        Some(m.toJsonObject)
    }
  }

  def getContentType(node: Node): String = {
    dcmiTypes.getOrElse(node.text, "")
  }
}
