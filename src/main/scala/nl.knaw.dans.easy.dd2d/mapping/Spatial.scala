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

trait Spatial {
  val NAMESPACE_GML = "http://www.opengis.net/gml"

  /** coordinate order y, x = latitude (DCX_SPATIAL_Y), longitude (DCX_SPATIAL_X) */
  val DEGREES_SRS_NAME = "http://www.opengis.net/def/crs/EPSG/0/4326"

  /** coordinate order x, y = longitude (DCX_SPATIAL_X), latitude (DCX_SPATIAL_Y) */
  val RD_SRS_NAME = "http://www.opengis.net/def/crs/EPSG/0/28992"

  val RD_SCHEME = "RD(in m.)"
  val LATLON_SCHEME = "latitude/longitude (m)"


  case class Point(x: String, y: String)

  protected def isRd(env: Node): Boolean = {
    // Not specifying a namespace in the attribut lookup because srsName is not recognized by the parser to be in the GML namespace,
    // even when the Envelope element has GML as its default namespace.
    env.attribute("srsName").exists(_.text == RD_SRS_NAME)
  }

  protected def getPoint(p: Node): Point = {
    val cs = p.text.trim.split("""\s+""")
    // make sure that you have valid numbers here
    cs(0).toDouble
    cs(1).toDouble
    Point(cs(0), cs(1))
  }
}

