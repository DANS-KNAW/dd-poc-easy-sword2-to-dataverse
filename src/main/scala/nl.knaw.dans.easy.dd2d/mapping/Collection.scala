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

object Collection extends BlockCollection with DebugEnhancedLogging {
  def toCollection(node: Node): JsonObject = {
    // TODO: also take attribute namespace into account (should be ddm)
    val optSubjectScheme = node.attribute("subjectScheme").flatMap(_.headOption).map(_.text).doIfNone(() => logger.error("Missing subjectScheme attribute on ddm:inCollection node"))
    val optSchemeUri = node.attribute("schemeURI").flatMap(_.headOption).map(_.text).doIfNone(() => logger.error("Missing schemeURI attribute on ddm:inCollection node"))
    val optValueUri = node.attribute("valueURI").flatMap(_.headOption).map(_.text).doIfNone(() => logger.error("Missing valueURI attribute on ddm:inCollectionr node"))
    val term = node.text

    val m = FieldMap()
    m.addPrimitiveField(COLLECTION_VOCABULARY, optSubjectScheme.get)
    m.addPrimitiveField(COLLECTION_VOCABULARY_URI, optSchemeUri.get)
    m.addPrimitiveField(COLLECTION_TERM, term)
    m.addPrimitiveField(COLLECTION_TERM_URI, optValueUri.get)
    m.toJsonObject
  }
}
