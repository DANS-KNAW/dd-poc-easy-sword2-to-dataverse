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

import nl.knaw.dans.easy.dd2d.{ InvalidSpatialSchemeException, TestSupportFixture }
import org.json4s.native.Serialization
import org.json4s.{ DefaultFormats, Formats }

import scala.util.{ Failure, Try }

class SpatialBoxSpec extends TestSupportFixture with BlockTemporalAndSpatial {
  private implicit val jsonFormats: Formats = new DefaultFormats {}

  "toEasyTsmSpatialBoxValueObject" should "create correct spatial box details in Json object" in {
    val spatialBox =
      <gml:boundedBy>
          <gml:Envelope srsName="http://www.opengis.net/def/crs/EPSG/0/4326">
            <gml:lowerCorner>91232.015554 436172.485680</gml:lowerCorner>
            <gml:upperCorner>121811.885272 486890.494251</gml:upperCorner>
          </gml:Envelope>
      </gml:boundedBy>
    val result = Serialization.writePretty(SpatialBox.toEasyTsmSpatialBoxValueObject(spatialBox).get)
    findString(result, s"$SPATIAL_BOX_SCHEME.value") shouldBe "latitude/longitude (m)"
    findString(result, s"$SPATIAL_BOX_NORTH.value") shouldBe "486890.494251"
    findString(result, s"$SPATIAL_BOX_EAST.value") shouldBe "91232.015554"
    findString(result, s"$SPATIAL_BOX_SOUTH.value") shouldBe "436172.485680"
    findString(result, s"$SPATIAL_BOX_WEST.value") shouldBe "121811.885272"
  }

  it should "give 'RD(in m.)' as spatial box scheme" in {
    val spatialBox =
      <gml:boundedBy>
          <gml:Envelope srsName="http://www.opengis.net/def/crs/EPSG/0/28992">
            <gml:lowerCorner>469470 209942</gml:lowerCorner>
            <gml:upperCorner>469890 209914</gml:upperCorner>
          </gml:Envelope>
      </gml:boundedBy>
    val result = Serialization.writePretty(SpatialBox.toEasyTsmSpatialBoxValueObject(spatialBox).get)
    findString(result, s"$SPATIAL_BOX_SCHEME.value") shouldBe "RD(in m.)"
  }

  it should "throw exception when invalid spatial scheme" in {
    val spatialBox =
      <gml:boundedBy>
          <gml:Envelope srsName="http://www.opengis.net/def/crs/EPSG/0/999999999">
            <gml:lowerCorner>469470 209942</gml:lowerCorner>
            <gml:upperCorner>469890 209914</gml:upperCorner>
          </gml:Envelope>
      </gml:boundedBy>
    inside(Try(SpatialBox.toEasyTsmSpatialBoxValueObject(spatialBox).get)) {
      case Failure(e: InvalidSpatialSchemeException) => e.getMessage should include("Invalid spatial scheme (http://www.opengis.net/def/crs/EPSG/0/999999999) as attribute of element 'Envelope'")
    }
  }

  it should "throw exception when longitude latitude pair is given incorrectly" in {
    val spatialBox =
      <gml:boundedBy>
          <gml:Envelope srsName="http://www.opengis.net/def/crs/EPSG/0/28992">
            <gml:lowerCorner>469470, 209942</gml:lowerCorner>
            <gml:upperCorner>469890, 209914</gml:upperCorner>
          </gml:Envelope>
      </gml:boundedBy>
    inside(Try(SpatialBox.toEasyTsmSpatialBoxValueObject(spatialBox).get)) {
      case Failure(e: NumberFormatException) => e.getMessage should include("469470,")
    }
  }

}
