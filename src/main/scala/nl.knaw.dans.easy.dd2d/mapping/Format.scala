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

import scala.xml.Node

object Format extends BlockContentTypeAndFileFormat {

  val imtFormats = Map(
    "application/postscript" -> "application/postscript",
    "application/rtf" -> "application/rtf",
    "application/pdf" -> "application/pdf",
    "application/msword" -> "application/msword",
    "text/plain" -> "text/plain",
    "text/html" -> "text/html",
    "text/sgml" -> "text/sgml",
    "text/xml" -> "text/xml",
    "image/jpeg" -> "image/jpeg",
    "image/gif" -> "image/gif",
    "image/tiff" -> "image/tiff",
    "video/quicktime" -> "video/quicktime",
    "video/mpeg1" -> "video/mpeg1"
  )

  def toContentTypeAndFileFormatBlockFormat(node: Node): JsonObject = {
    val contentFormat = getContentFormat(node)
    val m = FieldMap()
    m.addPrimitiveField(FORMAT_CV_VALUE, contentFormat)
    m.addPrimitiveField(FORMAT_CV_VOCABULARY, DCMI_FORMAT)
    if (contentFormat.equals("Other"))
      m.addPrimitiveField(FORMAT_CV_VOCABUALRY_URL, DCMI_FORMAT_BASE_URL)
    else
      m.addPrimitiveField(FORMAT_CV_VOCABUALRY_URL, DCMI_FORMAT_BASE_URL + contentFormat)
    m.toJsonObject
  }

  def getContentFormat(node: Node): String = {
    imtFormats.getOrElse(node.text, "Other")
  }
}
