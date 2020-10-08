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

import nl.knaw.dans.easy.dd2d.dataverse.json.{ JsonObject, createCvFieldSingleValue, createPrimitiveFieldSingleValue }

import scala.xml.Node

object Relation {
  private val ddmSchemeToDataverseScheme = Map(
    "DOI" -> "doi",
    "URN" -> "urn:nbn:nl",
    "ISBN" -> "ISBN",
    "ISSN" -> "ISSN",
    "NWO-PROJECTNR" -> "NWO ProjectNr",
    "ARCHIS-ZAAK-IDENTIFICATIE" -> "other",
    "MENDELEY-DATA" -> "other"
  )

  private val labelToRelationType = Map (
    "relation"-> "relation",
    "conformsTo" -> "conforms to",
    "hasFormat" -> "has format",
    "hasPart" -> "has part",
    "references" -> "references",
    "replaces" -> "replaces",
    "requires" -> "requires",
    "hasVersion" -> "has version",
    "isFormatOf" -> "is format of"
  )

  def isRelation(node: Node): Boolean = {
    labelToRelationType.keySet.contains(node.label)
  }


  def isRelatedIdentifier(node: Node): Boolean = {
    getScheme(node).isDefined
  }

  def toRelatedIdentifierValueObject(node: Node): JsonObject = {
    getScheme(node).map {
      s => Map(
        "easy-relid-relation" -> createCvFieldSingleValue("easy-relid-relation", getRelationType(node)),
        "easy-relid-type" -> createCvFieldSingleValue("easy-relid-type", s),
        "easy-relid-relatedid" -> createPrimitiveFieldSingleValue("easy-relid-relatedid", node.text)
      )
    }.getOrElse {
      throw new RuntimeException("Expected related identifier scheme")
    }
  }

  def toRelatedUrlValueObject(node: Node): JsonObject = {
    if (getScheme(node).isDefined) throw new RuntimeException("Expected related URL, not a related ID")
    else {
      Map(
        "easy-relid-relation-url" -> createCvFieldSingleValue("easy-relid-relation-url", getRelationType(node)),
        "easy-relid-url-title" -> createPrimitiveFieldSingleValue("easy-relid-url-title", node.text),
        "easy-relid-url-url" -> createPrimitiveFieldSingleValue("easy-relid-url-url", getHref(node).getOrElse(""))
      )
    }
  }

  private def getHref(node: Node): Option[String] = {
    node.attribute("href").flatMap(_.headOption.map(_.text))
  }

  private def getScheme(node: Node): Option[String] = {
    node.attribute("scheme").map(_.text.split(":").last).map(ddmSchemeToDataverseScheme)
  }

  private def getRelationType(node: Node): String = {
    labelToRelationType.getOrElse(node.label, "relation")
  }
}
