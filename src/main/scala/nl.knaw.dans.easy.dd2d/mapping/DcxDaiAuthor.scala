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

import nl.knaw.dans.easy.dd2d.dataverse.json.{ Field, PrimitiveFieldSingleValue, ValueObject, createCvFieldSingleValue, createPrimitiveFieldSingleValue }
import org.apache.commons.lang.StringUtils

import scala.collection.mutable
import scala.xml.Node

object DcxDaiAuthor extends Contributor {
  // TODO: handle case where a DcxDaiOrganization is specified

  private case class Author(titles: Option[String],
                            initials: Option[String],
                            insertions: Option[String],
                            surname: Option[String],
                            dai: Option[String],
                            isni: Option[String],
                            orcid: Option[String],
                            role: Option[String],
                            organization: Option[String])


  private def parseAuthor(authorElement: Node) = Author(
    titles = (authorElement \ "titles").map(_.text).headOption,
    initials = (authorElement \ "initials").map(_.text).headOption,
    insertions = (authorElement \ "insertions").map(_.text).headOption,
    surname = (authorElement \ "surname").map(_.text).headOption,
    dai = (authorElement \ "DAI").map(_.text).headOption,
    isni = (authorElement \ "ISNI").map(_.text).headOption,
    orcid = (authorElement \ "ORCID").map(_.text).headOption,
    role = (authorElement \ "role").map(_.text).headOption,
    organization = (authorElement \ "organization" \ "name").map(_.text).headOption)

  def toAuthorValueObject(node: Node): ValueObject = {
    val valueObject = mutable.Map[String, Field]()
    val author = parseAuthor(node)
    val name = formatName(author)

    if (StringUtils.isNotBlank(name)) {
      valueObject.put("authorName", createPrimitiveFieldSingleValue("authorName", name))
    }

    if (author.orcid.isDefined) {
      addIdentifier(valueObject, "ORCID", author.orcid.get)
    }
    else if (author.isni.isDefined) {
      addIdentifier(valueObject, "ISNI", author.isni.get)
    }
         else if (author.dai.isDefined) {
           addIdentifier(valueObject, "DAI", author.dai.get)
         }

    if (author.organization.isDefined) {
      valueObject.put("authorAffiliation", createPrimitiveFieldSingleValue("authorAffiliation", author.organization.get))
    }
    valueObject.toMap
  }

  def toContributorValueObject(node: Node): ValueObject = {
    val valueObject = mutable.Map[String, Field]()
    val author = parseAuthor(node)
    val name = formatName(author)
    if (StringUtils.isNotBlank(name)) {
      valueObject.put("contributorName", createPrimitiveFieldSingleValue("contributorName", name))
    }
    else if (author.organization.isDefined) {
      valueObject.put("contributorName", createPrimitiveFieldSingleValue("contributorName", name))
    }
    if(author.role.isDefined) {
      valueObject.put("contributorType",
        createCvFieldSingleValue("contributorType", author.role.map(contributoreRoleToContributorType.getOrElse(_, "Other")).getOrElse("Other")))
    }
    valueObject.toMap
  }

  private def formatName(author: Author): String = {
    List(author.titles.getOrElse(""),
      author.initials.getOrElse(""),
      author.insertions.getOrElse(""),
      author.surname.getOrElse(""))
      .mkString(" ").trim().replaceAll("\\s+", " ")
  }

  private def addIdentifier(valueObject: mutable.Map[String, Field], scheme: String, value: String): Unit = {
    if (StringUtils.isNotBlank(value)) {
      valueObject.put("authorIdentifierScheme", PrimitiveFieldSingleValue("authorIdentifierScheme", multiple = false, "controlledVocabulary", scheme))
      valueObject.put("authorIdentifier", createPrimitiveFieldSingleValue("authorIdentifier", value))
    }
  }
}
