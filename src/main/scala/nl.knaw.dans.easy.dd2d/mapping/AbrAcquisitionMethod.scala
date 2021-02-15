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

import scala.util.Try
import scala.xml.Node

object AbrAcquisitionMethod extends BlockArchaeologySpecific with AbrScheme with DebugEnhancedLogging {

  def toVerwervingswijze(node: Node): Try[JsonObject] = Try {
    // TODO: also take attribute namespace into account (should be ddm)
    val optSubjectScheme = node.attribute("subjectScheme").flatMap(_.headOption).map(_.text).doIfNone(() => logger.error("Missing subjectScheme attribute on ddm:acquisitionMethod node"))
    val optSchemeUri = node.attribute("schemeURI").flatMap(_.headOption).map(_.text).doIfNone(() => logger.error("Missing schemeURI attribute on ddm:acquisitionMethod node"))
    val optValueUri = node.attribute("valueURI").flatMap(_.headOption).map(_.text).doIfNone(() => logger.error("Missing valueURI attribute on ddm:acquisitionMethod node"))
    val term = node.text

    val m = FieldMap()
    m.addPrimitiveField(SCHEME_ABR_VERWERVINGSWIJZE, optSubjectScheme.get)
    m.addPrimitiveField(SCHEME_URI_ABR_VERWERVINGSWIJZE, optSchemeUri.get)
    m.addPrimitiveField(ABR_VERWERVINGSWIJZE_TERM, term)
    m.addPrimitiveField(ABR_VERWERVINGSWIJZE_TERM_URI, optValueUri.get)
    m.toJsonObject
  }

  /**
   * Predicate to select only the elements that can be processed by [[AbrAcquisitionMethod.toVerwervingswijze]].
   *
   * @param node the node to examine
   * @return
   */
  def isAbrVerwervingswijze(node: Node): Boolean = {
    // TODO: also take attribute namespace into account (should be ddm)
    // TODO: correct the scheme: should be 'ABR Period' ??
    node.label == "acquisitionMethod" && hasAttribute(node, "subjectScheme", SCHEME_ABR_VERWERVINGSWIJZE) && hasAttribute(node, "schemeURI", SCHEME_URI_ABR_VERWERVINGSWIJZE)
  }
}
