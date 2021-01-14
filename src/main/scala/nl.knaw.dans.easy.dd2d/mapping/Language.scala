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

object Language extends BlockBasicInformation with DebugEnhancedLogging {
  private val iso639_2ToDataverse = Map(
    "eng" -> "English",
    "nld" -> "Dutch",
    "dut" -> "Dutch",
    "fre" -> "French",
    "fra" -> "French",
    "ger" -> "German",
    "deu" -> "German"
    // TODO: extend, and probably load from resource file
  )

  private val iso639_1ToIso639_2 = Map(
    "en" -> "eng",
    "nl" -> "dut",
    "fr" -> "fre",
    "de" -> "ger"
    // TODO: extend, and probably load from resource file
  )

  def toBasicInformationBlockLanguageOfFiles(node: Node): Option[JsonObject] = {

    iso639_2ToDataverse.get(node.text)
      .map(isoLanguage => {
        val m = FieldMap()
        m.addPrimitiveField(LANGUAGE_OF_FILES_CV_VALUE, isoLanguage)
        m.addPrimitiveField(LANGUAGE_OF_FILES_CV_VOCABULARY, LANGUAGE_OF_FILES_CV_VOCABULARY_NAME)
        m.addPrimitiveField(LANGUAGE_OF_FILES_CV_VOCABULART_URL, LANGUAGE_CV_ISO_639_2_URL + node.text)
        m.toJsonObject
      })
  }

  def toBasicInformationLanguageOfMetadata(node: Node): Option[JsonObject] = {
    //ISO 639-1: two letter country codes
    val xmlLangAttribute = getXmlLangAttribute(node)
    //ISO 639-2: three letter country codes. Only these are resolvable when used in term url.
    val iso639_2LangAttribute = iso639_1ToIso639_2.getOrElse(xmlLangAttribute, "")

    iso639_2ToDataverse.get(iso639_2LangAttribute)
      .map(isoLanguage => {
        val m = FieldMap()
        m.addPrimitiveField(LANGUAGE_OF_METADATA_CV_VALUE, isoLanguage)
        m.addPrimitiveField(LANGUAGE_OF_METADATA_CV_VOCABULARY, LANGUAGE_OF_FILES_CV_VOCABULARY_NAME)
        m.addPrimitiveField(LANGUAGE_OF_METADATA_CV_VOCABULART_URL, LANGUAGE_CV_ISO_639_2_URL + iso639_2LangAttribute)
        m.toJsonObject
      }).doIfNone(() => logger.error(s"Invalid controlled vocabulary term for 'Language of Metadata'"))
  }

  def isISOLanguage(node: Node): Boolean = {
    hasXsiType(node, "ISO639-2")
  }

  def getXmlLangAttribute(ddm: Node): String = {
    (ddm \\ "_").filter(_.attributes.nonEmpty).map(_.attributes).find(_.key.contains("lang")).map(_.value).getOrElse("").toString
  }

  def toCitationBlockLanguage(node: Node): Option[String] = {
    if (hasXsiType(node, "ISO639-2")) iso639_2ToDataverse.get(node.text)
    else Option.empty[String] // TODO: try to map to Dataverse vocabulary?
  }
}
