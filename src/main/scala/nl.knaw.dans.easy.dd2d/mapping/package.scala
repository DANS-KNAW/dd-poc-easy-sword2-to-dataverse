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
package nl.knaw.dans.easy.dd2d

import org.joda.time.format.{ DateTimeFormat, DateTimeFormatter }

import scala.xml.Node

package object mapping {
  private val XML_SCHEMA_INSTANCE_URI = "http://www.w3.org/2001/XMLSchema-instance"

  /**
   * Returns whether the node has an xsi:type attribute with the specified type. Note that namespace-prefix of the *value* is ignored.
   *
   * @param node the node to examine
   * @param xsiType the xsiType to look fore
   * @return true or false
   */
  def hasXsiType(node: Node, xsiType: String): Boolean = {
    node.attribute(XML_SCHEMA_INSTANCE_URI, "type").map(_.text).map(t => t.endsWith(s":$xsiType") || t == xsiType).exists(identity)
  }

}
