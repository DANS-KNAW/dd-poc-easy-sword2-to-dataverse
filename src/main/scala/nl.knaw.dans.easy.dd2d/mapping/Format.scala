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

import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.xml.Node

object Format extends BlockContentTypeAndFileFormat with DebugEnhancedLogging {

  val imtFormats = List(
    "application/postscript",
    "application/rtf",
    "application/pdf",
    "application/msword",
    "text/plain",
    "text/html",
    "text/sgml",
    "text/xml",
    "image/jpeg",
    "image/gif",
    "image/tiff",
    "video/quicktime",
    "video/mpeg1"
  )

  def toContentTypeAndFileFormatBlockFormat(node: Node): Option[JsonObject] = {
    val contentFormat = node.text
    if (imtFormats.contains(contentFormat)) {
      val m = FieldMap()
      m.addPrimitiveField(FORMAT_CV_VALUE, node.text)
      m.addPrimitiveField(FORMAT_CV_VOCABULARY, DCMI_FORMAT)
      m.addPrimitiveField(FORMAT_CV_VOCABUALRY_URL, DCMI_FORMAT_BASE_URL + contentFormat)
      Some(m.toJsonObject)
    }
    else {
      logger.error(s"Invalid controlled vocabulary term for 'Format (Media Type)': '$contentFormat'")
      None
    }
  }
}
