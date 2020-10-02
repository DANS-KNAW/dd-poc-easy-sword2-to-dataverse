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

import nl.knaw.dans.easy.dd2d.dataverse.json.{ Field, PrimitiveFieldSingleValue, ValueObject, createPrimitiveFieldSingleValue }
import org.apache.commons.lang.StringUtils

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.xml.NodeSeq

case class CreatorDetailsNodes(nodes: NodeSeq) {
  def toAuthorValueObjects: List[ValueObject] = {
    val valueObjects = new ListBuffer[ValueObject]()
    nodes
      .foreach(e => {
        val authorElement = (e \ "author")
        val titles = (authorElement \ "titles").map(_.text).headOption.getOrElse("")
        val initials = (authorElement \ "initials").map(_.text).headOption.getOrElse("")
        val insertions = (authorElement \ "insertions").map(_.text).headOption.getOrElse("")
        val surname = (authorElement \ "surname").map(_.text).headOption.getOrElse("")
        val dai = (authorElement \ "DAI").map(_.text).headOption.getOrElse("")
        val isni = (authorElement \ "ISNI").map(_.text).headOption.getOrElse("")
        val orcid = (authorElement \ "ORCID").map(_.text).headOption.getOrElse("")
        val organization = (authorElement \ "organization" \ "name").map(_.text).headOption.getOrElse("")
        val name = List(titles, initials, insertions, surname).mkString(" ").trim().replaceAll("\\s+", " ")
        val valueObject = mutable.Map[String, Field]()

        if (StringUtils.isNotBlank(name)) {
          valueObject.put("authorName", createPrimitiveFieldSingleValue("authorName", name))
        }

        if (StringUtils.isNotBlank(orcid)) {
          addIdentifier(valueObject, "ORCID", orcid)
        }
        else if (StringUtils.isNotBlank(isni)) {
          addIdentifier(valueObject, "ISNI", isni)
        }
             else if (StringUtils.isNotBlank(dai)) {
               addIdentifier(valueObject, "DAI", dai)
             }

        if (StringUtils.isNotBlank(organization)) {
          valueObject.put("authorAffiliation", createPrimitiveFieldSingleValue("authorAffiliation", organization))
        }
        valueObjects += valueObject.toMap
      })
    valueObjects.toList
  }

  private def addIdentifier(valueObject: mutable.Map[String, Field], scheme: String, value: String): Unit = {
    if (StringUtils.isNotBlank(value)) {
      valueObject.put("authorIdentifierScheme", PrimitiveFieldSingleValue("authorIdentifierScheme", multiple = false, "controlledVocabulary", scheme))
      valueObject.put("authorIdentifier", createPrimitiveFieldSingleValue("authorIdentifier", value))
    }
  }
}
