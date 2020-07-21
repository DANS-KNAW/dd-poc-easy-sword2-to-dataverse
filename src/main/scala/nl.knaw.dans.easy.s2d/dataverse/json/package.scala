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
package nl.knaw.dans.easy.s2d.dataverse

import org.json4s.{ DefaultFormats, Formats }

package object json {
  type MetadataBlockName = String

  abstract class Field
  implicit val jsonFormats: Formats = DefaultFormats
  case class MetadataBlock(displayName: String, fields: List[Field])

  case class DatasetVersion(metadataBlocks: Map[MetadataBlockName, MetadataBlock])
  case class DataverseDataset(datasetVersion: DatasetVersion)

  case class PrimitiveFieldSingleValue(typeName: String,
                                       multiple: Boolean,
                                       typeClass: String, // TODO: Can be nothing else; how to fix this value? Subclassing doesn't seem to work.
                                       value: String
                                      //create enum
                                      ) extends Field

  case class PrimitiveFieldMultipleValues(typeName: String,
                                          multiple: Boolean,
                                          //create enum
                                          typeClass: String,
                                          value: List[String]
                                         ) extends Field


  case class CompoundField(typeName: String,
                           multiple: Boolean,
                           typeClass: String = "compound", // TODO: idem
                           value: List[scala.collection.mutable.Map[String, Field]]) extends Field

}
