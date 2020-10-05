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

import nl.knaw.dans.easy.dd2d.dataverse.json.{ Field, ValueObject, createCvFieldSingleValue, createPrimitiveFieldSingleValue }
import nl.knaw.dans.easy.dd2d.mapping.DcxDaiAuthor.{ contributoreRoleToContributorType, formatName, parseAuthor }
import org.apache.commons.lang.StringUtils

import scala.collection.mutable
import scala.xml.Node

object DcxDaiOrganization extends Contributor {
  private case class Organization(name: Option[String],
                                  role: Option[String])

  private def parseOrganization(node: Node): Organization = {
    Organization(name = (node \ "name").map(_.text).headOption,
      role = (node \ "role").map(_.text).headOption)
  }

  def toContributorValueObject(node: Node): ValueObject = {
    val valueObject = mutable.Map[String, Field]()
    val organization = parseOrganization(node)
    if (organization.name.isDefined) {
      valueObject.put("contributorName", createPrimitiveFieldSingleValue("contributorName", organization.name.get))
    }
    if (organization.role.isDefined) {
      valueObject.put("contributorType",
        createCvFieldSingleValue("contributorType", organization.role.map(contributoreRoleToContributorType.getOrElse(_, "Other")).getOrElse("Other")))
    }
    valueObject.toMap
  }
}
