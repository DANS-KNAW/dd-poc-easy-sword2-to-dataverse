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

object Language extends BlockBasicInformation with DebugEnhancedLogging {
  private val shortIsoToDataverse = Map(
    "eng" -> "English",
    "nld" -> "Dutch",
    "dut" -> "Dutch",
    "fre" -> "French",
    "fra" -> "French",
    "ger" -> "German",
    "deu" -> "German"
    // TODO: extend, and probably load from resource file
  )

  def toBasicInformationBlockLanguageOfFiles(node: Node): Option[JsonObject] = {
    val isoLanguage = getISOLanguage(node)
    isoLanguage match {
      case "" =>
        None
      case _ =>
        val m = FieldMap()
        m.addPrimitiveField(LANGUAGE_OF_FILES_CV_VALUE, isoLanguage)
        m.addPrimitiveField(LANGUAGE_OF_FILES_CV_VOCABULARY, LANGUAGE_OF_FILES_CV_VOCABULARY_NAME)
        m.addPrimitiveField(LANGUAGE_OF_FILES_CV_VOCABULART_URL, LANGUAGE_CV_ISO_639_2_URL + node.text)
        Some(m.toJsonObject)
    }
  }

  def toBasicInformationLanguageOfMetadata(node: Node, depositDirName: String): Option[JsonObject] = {
    val isoLanguageAttribute = getISOLanguaAttribute(node)
    val isoLanguage = shortIsoToDataverse.getOrElse(isoLanguageAttribute, "")
    isoLanguage match {
      case "" =>
        logger.error(s"Invalid controlled vocabulary term for 'Language of Metadata' for the deposit '$depositDirName'" )
        None
      case _ =>
        val m = FieldMap()
        m.addPrimitiveField(LANGUAGE_OF_METADATA_CV_VALUE, isoLanguage) fork
        m.addPrimitiveField(LANGUAGE_OF_METADATA_CV_VOCABULARY, LANGUAGE_OF_FILES_CV_VOCABULARY_NAME)
        m.addPrimitiveField(LANGUAGE_OF_METADATA_CV_VOCABULART_URL, LANGUAGE_CV_ISO_639_2_URL + isoLanguageAttribute)
        Some(m.toJsonObject)
    }
  }

  def isISOLanguage(node: Node): Boolean = {
    hasXsiType(node, "ISO639-2")
  }

  def getISOLanguage(node: Node): String = {
    shortIsoToDataverse.getOrElse(node.text, "")
  }

  def getISOLanguaAttribute(ddm: Node): String = {
    (ddm \\ "title").headOption.flatMap(_.attribute(XML_LANGUAGE_PROVISIONAL_URI, "lang")).getOrElse("").toString
  }

  def toCitationBlockLanguage(node: Node): Option[String] = {
    if (hasXsiType(node, "ISO639-2")) shortIsoToDataverse.get(node.text)
    else Option.empty[String] // TODO: try to map to Dataverse vocabulary?
  }
}
