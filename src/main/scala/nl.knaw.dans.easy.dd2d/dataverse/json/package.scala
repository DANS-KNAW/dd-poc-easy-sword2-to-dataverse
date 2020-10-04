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

import better.files.File
import org.json4s.{ DefaultFormats, Formats }

package object json {
  type MetadataBlockName = String
  type ValueObject = Map[String, Field]

  abstract class Field
  implicit val jsonFormats: Formats = DefaultFormats
  case class MetadataBlock(displayName: String, fields: List[Field])

  case class DatasetVersion(metadataBlocks: Map[MetadataBlockName, MetadataBlock])
  case class DataverseDataset(datasetVersion: DatasetVersion)

  case class PrimitiveFieldSingleValue(typeName: String,
                                       multiple: Boolean,
                                       typeClass: String,
                                       value: String
                                      ) extends Field

  case class PrimitiveFieldMultipleValues(typeName: String,
                                          multiple: Boolean,
                                          //create enum
                                          typeClass: String,
                                          value: List[String]
                                         ) extends Field

  case class CompoundField(typeName: String,
                           multiple: Boolean,
                           typeClass: String = "compound",
                           value: List[Map[String, Field]]) extends Field

  case class FileMetadata(description: Option[String] = None,
                          directoryLabel: Option[String] = None,
                          restrict: Option[String] = None)

  case class FileInformation(file: File, fileMetadata: FileMetadata)

  def createPrimitiveFieldSingleValue(name: String, value: String): PrimitiveFieldSingleValue = {
    PrimitiveFieldSingleValue(name, multiple = false, "primitive", value)
  }

  def createPrimitiveFieldMultipleValues(name: String, values: List[String]): PrimitiveFieldMultipleValues = {
    PrimitiveFieldMultipleValues(name, multiple = true, "primitive", values)
  }

  def createCvFieldSingleValue(name: String, value: String): PrimitiveFieldSingleValue = {
    PrimitiveFieldSingleValue(name, multiple = false, "controlledVocabulary", value)
  }

  def createCvFieldMultipleValues(name: String, values: List[String]): PrimitiveFieldMultipleValues = {
    PrimitiveFieldMultipleValues(name, multiple = true, "controlledVocabulary", values)
  }

  def createCompoundFieldSingleValue(name: String, value: Map[String, Field]): CompoundField = {
    CompoundField(name, multiple = false, typeClass = "compound", value = List(value))
  }

  def createCompoundFieldMultipleValues(name: String, values: List[Map[String, Field]]): CompoundField = {
    CompoundField(name, multiple = true, typeClass = "compound", values)
  }
}
