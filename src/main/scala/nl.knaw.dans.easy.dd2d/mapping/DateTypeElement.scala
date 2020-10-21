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

import nl.knaw.dans.easy.dd2d.dataverse.json.{ JsonObject, FieldMap, createCvFieldSingleValue, createPrimitiveFieldSingleValue }
import org.joda.time.DateTime
import org.joda.time.format.{ DateTimeFormat, DateTimeFormatter }

import scala.xml.Node

object DateTypeElement extends BlockBasicInformation {
  private val NAMESPACE_DCTERMS = "http://purl.org/dc/terms/"

  private val yyyymmddPattern: DateTimeFormatter = DateTimeFormat.forPattern("YYYY-MM-dd")

  private val labelToDateType = Map(
    "date" -> "Date",
    "valid" -> "Valid",
    "issued" -> "Issued",
    "modified" -> "Modified",
    "dateAccepted" -> "Date accepted",
    "dateCopyrighted" -> "Date copyrighted"
  )

  def isDate(node: Node): Boolean = {
    labelToDateType.keySet.contains(node.label)
  }

  def hasW3CFormat(node: Node): Boolean = {
    hasXsiType(node, "W3CDTF")
  }

  def toYearMonthDayFormat(node: Node): Option[String] = Option {
    yyyymmddPattern.print(DateTime.parse(node.text))
  }

  def toBasicInfoFormattedDateValueObject(node: Node): JsonObject = {
    val m = FieldMap()
    m.addCvField(DATE_TYPE, labelToDateType.getOrElse(node.label, "Date"))
    m.addPrimitiveField(DATE_VALUE, toYearMonthDayFormat(node).getOrElse(""))
    m.toJsonObject
  }

  def toBasicInfoFreeDateValue(node: Node): JsonObject = {
    val m = FieldMap()
    m.addCvField(DATE_FREE_FORMAT_TYPE, labelToDateType.getOrElse(node.label, "Date"))
    m.addPrimitiveField(DATE_FREE_FORMAT_VALUE, node.text)
    m.toJsonObject
  }
}
