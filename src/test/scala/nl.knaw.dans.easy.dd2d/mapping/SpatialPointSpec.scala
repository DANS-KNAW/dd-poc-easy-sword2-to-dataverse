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

class SpatialPointSpec extends TestSupportFixture with BlockTemporalAndSpatial {
  private implicit val jsonFormats: Formats = new DefaultFormats {}

  "toEasyTsmSpatialPointValueObject" should "create correct spatial point details in Json object" in {
    val spatialPoint = <gml:pos srsName="http://www.opengis.net/def/crs/EPSG/0/4326">52.08113 4.34510</gml:pos>
    val result = Serialization.writePretty(SpatialPoint.toEasyTsmSpatialPointValueObject(spatialPoint).get)
    findString(result, s"$SPATIAL_POINT_SCHEME.value") shouldBe "latitude/longitude (m)"
    findString(result, s"$SPATIAL_POINT_X.value") shouldBe "52.08113"
    findString(result, s"$SPATIAL_POINT_Y.value") shouldBe "4.34510"
  }

  it should "give 'RD(in m.)' as spatial point scheme and coordinates as integers" in {
    val spatialPoint = <gml:pos srsName="http://www.opengis.net/def/crs/EPSG/0/28992">469470 209942</gml:pos>
    val result = Serialization.writePretty(SpatialPoint.toEasyTsmSpatialPointValueObject(spatialPoint).get)
    findString(result, s"$SPATIAL_POINT_SCHEME.value") shouldBe "RD(in m.)"
    findString(result, s"$SPATIAL_POINT_X.value") shouldBe "469470"
    findString(result, s"$SPATIAL_POINT_Y.value") shouldBe "209942"
  }

  it should "throw exception when invalid spatial scheme" in {
    val spatialPoint = <gml:pos srsName="http://www.opengis.net/def/crs/EPSG/0/9999999">52.08113, 4.34510</gml:pos>
    inside(Try(SpatialPoint.toEasyTsmSpatialPointValueObject(spatialPoint).get)) {
      case Failure(e: InvalidSpatialSchemeException) => e.getMessage should include("Invalid spatial scheme (http://www.opengis.net/def/crs/EPSG/0/9999999) as attribute of element 'pos'")
    }
  }

  it should "throw exception when spatial point coordinates are given incorrectly" in {
    val spatialPoint = <gml:pos srsName="http://www.opengis.net/def/crs/EPSG/0/28992">52.08113, 4.34510</gml:pos>
    inside(Try(SpatialPoint.toEasyTsmSpatialPointValueObject(spatialPoint).get)) {
      case Failure(e: NumberFormatException) => e.getMessage should include("52.08113,")
    }
  }

}
