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

import java.net.URI
import java.nio.file.Paths
import scala.xml.Node

object AbrReportType extends BlockArchaeologySpecific with AbrScheme with DebugEnhancedLogging {

  def toAbrRapportType(node: Node): Option[String] = {
    // TODO: also take attribute namespace into account (should be ddm)
    node.attribute("valueURI").flatMap(_.headOption).map(_.text).doIfNone(() => logger.error("Missing valueURI attribute on ddm:reportNumber node"))
  }

  def getIdFormValueUri(uri: URI): String = {
    Paths.get(uri.getPath).getFileName.toString
  }

  /**
   * Predicate to select only the elements that can be processed by [[AbrReportType.toAbrRapportType]].
   *
   * @param node the node to examine
   * @return
   */
  def isAbrReportType(node: Node): Boolean = {
    // TODO: also take attribute namespace into account (should be ddm)
    // TODO: correct the scheme: should be 'ABR Period' ??
    node.label == "reportNumber" && hasAttribute(node, "subjectScheme", SCHEME_ABR_RAPPORT_TYPE) && hasAttribute(node, "schemeURI", SCHEME_URI_ABR_RAPPORT_TYPE)
  }
}
