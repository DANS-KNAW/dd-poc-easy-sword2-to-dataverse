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

import scala.xml.{ Elem, Node }

/**
 * ddm:audience element with a NARCIS classification code in it.
 * Used for Subject field in the Citation metadata block
 */
object Audience extends BlockBasicInformation with DebugEnhancedLogging {
  val narcisToSubject = Map(
    "D11" -> "Mathematical Sciences",
    "D12" -> "Physics",
    "D13" -> "Chemistry",
    "D14" -> "Engineering",
    "D16" -> "Computer and Information Science",
    "D17" -> "Astronomy and Astrophysics",
    "D18" -> "Agricultural Sciences",
    "D2" -> "Medicine, Health and Life Sciences",
    "D3" -> "Arts and Humanities",
    "D4" -> "Law",
    "D6" -> "Social Sciences",
    "D7" -> "Business and Management",
    "E15" -> "Earth and Environmental Sciences"
  )

  /**
   * Creates a Map with Subject CV fields for a Compound Field
   *
   * @param node                 the audience element
   * @param narcisClassification the Narcis classification
   * @return A JsonObject with Subject CV fields
   */
  def toBasicInformationBlockSubjectCv(node: Node, narcisClassification: Elem): Option[JsonObject] = {
    getTermAndUrl(node, narcisClassification)
      .map(termAndUrl => {
        val m = FieldMap()
        m.addPrimitiveField(SUBJECT_CV_VALUE, termAndUrl.term)
        m.addPrimitiveField(SUBJECT_CV_VOCABULARY, SUBJECT_NARCIS_CLASSIFICATION)
        m.addPrimitiveField(SUBJECT_CV_VOCABULARY_URI, termAndUrl.url)
        m.toJsonObject
      }).doIfNone(() => logger.error(s"Invalid controlled vocabulary term for 'Subject': ${ node.text }"))
  }

  /**
   * Gets term and url from the NARCIS classification
   *
   * @param node                 the audience element
   * @param narcisClassification the Narcis classification
   * @return the Dataverse subject term and url or None
   */
  private def getTermAndUrl(node: Node, narcisClassification: Elem): Option[TermAndUrl] = {
    (narcisClassification \ "Description")
      .find(_.attributes.exists(_.value.text contains node.text))
      .map(description => {
        val term = (description \ "prefLabel").find(_.attributes.exists(_.value.text == "en")).map(_.text).getOrElse("")
        val url = description.attributes.value.headOption.getOrElse(SUBJECT_NARCIS_CLASSIFICATION_URL).toString
        TermAndUrl(term, url)
      })
  }

  /**
   * Returns the best match for this NARCIS classification code in the Dataverse subject vocabulary
   * used in the Citation metadata block
   *
   * @param node the audience element
   * @return the Dataverse subject term
   */
  def toCitationBlockSubject(node: Node): Option[String] = {
    if (!node.text.matches("""^[D|E]\d{5}$""")) {
      throw new RuntimeException("NARCIS classification code format incorrect")
    }

    Option(narcisToSubject
      .find { case (k, _) => node.text.startsWith(k) }
      .getOrElse(("", "Other"))
      ._2)
  }
}
