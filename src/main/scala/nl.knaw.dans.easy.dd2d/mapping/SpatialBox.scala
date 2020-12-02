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

object SpatialBox extends Spatial with BlockTemporalAndSpatial {

  /**
   * Converts a boundedBy element to a spatial box value object.
   *
   * @param boundedBy the boundedBy element
   * @return the spatial box value object
   */
  def toEasyTsmSpatialBoxValueObject(boundedBy: Node): JsonObject = {
    val isRD = (boundedBy \ "Envelope").headOption.map(isRd).get // TODO: improve error handling
    val lowerCorner = (boundedBy \ "Envelope" \ "lowerCorner").headOption.map(getPoint).get // TODO: improve error handling
    val upperCorner = (boundedBy \ "Envelope" \ "upperCorner").headOption.map(getPoint).get // TODO: improve error handling
    val m = FieldMap()
    m.addCvField(SPATIAL_BOX_SCHEME, if (isRD) RD_SCHEME
                                     else LATLON_SCHEME)
    m.addPrimitiveField(SPATIAL_BOX_NORTH, upperCorner.y)
    m.addPrimitiveField(SPATIAL_BOX_EAST, lowerCorner.x)
    m.addPrimitiveField(SPATIAL_BOX_SOUTH, lowerCorner.y)
    m.addPrimitiveField(SPATIAL_BOX_WEST, upperCorner.x)
    m.toJsonObject
  }
}
