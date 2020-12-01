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
package nl.knaw.dans.easy.dd2d.dataverse

import nl.knaw.dans.lib.dataverse.model.dataset.{ CompoundField, ControlledMultipleValueField, ControlledSingleValueField, MetadataField, PrimitiveMultipleValueField, PrimitiveSingleValueField }
import org.json4s.{ DefaultFormats, Formats }

import scala.collection.mutable

package object json {
  type MetadataBlockName = String
  type JsonObject = Map[String, MetadataField]

  implicit val jsonFormats: Formats = DefaultFormats

  case class FieldMap() {
    private val fields = mutable.Map[String, MetadataField]()

    def addPrimitiveField(name: String, value: String): Unit = {
      fields.put(name, createPrimitiveFieldSingleValue(name, value))
    }

    def addCvField(name: String, value: String): Unit = {
      fields.put(name, createCvFieldSingleValue(name, value))
    }

    def addCompoundField(name: String, value: Map[String, MetadataField]): Unit = {
      fields.put(name, createCompoundFieldSingleValue(name, value))
    }

    def toJsonObject: JsonObject = fields.toMap
  }

  def createPrimitiveFieldSingleValue(name: String, value: String): PrimitiveSingleValueField = {
    PrimitiveSingleValueField(name, value)
  }

  def createPrimitiveFieldMultipleValues(name: String, values: List[String]): PrimitiveMultipleValueField = {
    PrimitiveMultipleValueField(name, values)
  }

  def createCvFieldSingleValue(name: String, value: String): ControlledSingleValueField = {
    ControlledSingleValueField("controlledVocabulary", name, multiple = false, value)
  }

  def createCvFieldMultipleValues(name: String, values: List[String]): ControlledMultipleValueField = {
    ControlledMultipleValueField(name, values)
  }

  def createCompoundFieldSingleValue(name: String, value: Map[String, MetadataField]): CompoundField = {
    CompoundField(name, value)
  }

  def createCompoundFieldMultipleValues(name: String, values: List[Map[String, MetadataField]]): CompoundField = {
    CompoundField(name, values)
  }
}
