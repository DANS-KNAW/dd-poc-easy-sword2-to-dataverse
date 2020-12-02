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

import scala.xml.Node

object DcxDaiOrganization extends Contributor with BlockCitation {
  private case class Organization(name: Option[String],
                                  role: Option[String])

  private def parseOrganization(node: Node): Organization = {
    Organization(name = (node \ "name").map(_.text).headOption,
      role = (node \ "role").map(_.text).headOption)
  }

  def toContributorValueObject(node: Node): JsonObject = {
    val m = FieldMap()
    val organization = parseOrganization(node)
    if (organization.name.isDefined) {
      m.addPrimitiveField(CONTRIBUTOR_NAME, organization.name.get)
    }
    if (organization.role.isDefined) {
      m.addCvField(CONTRIBUTOR_TYPE, organization.role.map(contributoreRoleToContributorType.getOrElse(_, "Other")).getOrElse("Other"))
    }
    m.toJsonObject
  }
}
