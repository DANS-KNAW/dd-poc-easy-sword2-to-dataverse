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
package nl.knaw.dans.easy.dd2d

import com.jayway.jsonpath.JsonPath

import scala.collection.JavaConverters._

trait JsonPathSupportFixture {

  /**
   * Reads the JSON code in `json` and extracts the string found using the JSON Path expression in `path`
   *
   * @param json the JSON code
   * @param path the JSON path
   * @return a string
   */
  protected def findString(json: String, path: String): String = {
    val ctx = JsonPath.parse(json)
    ctx.read("$." + path).asInstanceOf[String]
  }

  /**
   * Reads the JSON code in `json` and extracts the object found using the JSON Path expression in `path`
   *
   * @param json
   * @param path
   * @return
   */
  protected def findObject(json: String, path: String): Map[String, Any] = {
    val ctx = JsonPath.parse(json)
    ctx.read("$." + path).asInstanceOf[java.util.HashMap[String, Any]].asScala.toMap
  }
}
