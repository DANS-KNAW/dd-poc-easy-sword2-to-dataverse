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

import java.io.PrintStream
import java.net.URI

import better.files.File
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import scalaj.http.HttpResponse

import scala.util.Try

class FileCommand(id: String, isPersistentId: Boolean, configuration: DataverseInstanceConfig)(implicit resultOutput: PrintStream) extends HttpSupport with DebugEnhancedLogging {
  protected val connectionTimeout: Int = configuration.connectionTimeout
  protected val readTimeout: Int = configuration.readTimeout
  protected val baseUrl: URI = configuration.baseUrl
  protected val apiToken: String = configuration.apiToken
  protected val apiVersion: String = configuration.apiVersion

  def restrict(doRestict: Boolean): Try[HttpResponse[Array[Byte]]] = {
    trace(doRestict)
    val path = if (isPersistentId) s"files/:persistentId/restrict?persistentId=$id"
               else s"files/$id/restrict"
    put(path)(doRestict.toString)
  }

  def replace(replacementData: File, replacementJsonMetadata: Option[File], jsonString: Option[String]): Try[HttpResponse[Array[Byte]]] = {
    trace(replacementData, replacementJsonMetadata, jsonString)
    val path = if (isPersistentId) s"files/:persistentId/replace?persistentId=$id"
               else s"files/$id/replace"
    replacementJsonMetadata.map {
      f =>
        tryReadFileToString(f).flatMap {
          s => postFile(path, replacementData, Some(s))(200, formatResponseAsJson = true)
        }
    }.getOrElse {
      postFile(path, replacementData, jsonString)(200, formatResponseAsJson = true)
    }
  }

  def uningest(): Try[HttpResponse[Array[Byte]]] = {
    trace(())
    val path = if (isPersistentId) s"files/:persistentId/uningest?persistentId=$id"
               else s"files/$id/uningest"
    postJson(path)(200)()
  }

  def reingest(): Try[HttpResponse[Array[Byte]]] = {
    trace(())
    val path = if (isPersistentId) s"files/:persistentId/reingest?persistentId=$id"
               else s"files/$id/reingest"
    postJson(path)(200)()
  }

  def getProvenance(inJsonFormat: Boolean): Try[HttpResponse[Array[Byte]]] = {
    trace(inJsonFormat)
    val path = if (isPersistentId) s"files/:persistentId/prov-${
      if (inJsonFormat) "json"
      else "freeform"
    }?persistentId=$id"
               else s"files/$id/prov-${
                 if (inJsonFormat) "json"
                 else "freeform"
               }"
    get(path, formatResponseAsJson = inJsonFormat)
  }

  def setProvenacne(prov: String, inJsonFormat: Boolean): Try[HttpResponse[Array[Byte]]] = {
    trace(prov, inJsonFormat)
    val path = if (isPersistentId) s"files/:persistentId/prov-${
      if (inJsonFormat) "json"
      else "freeform"
    }?persistentId=$id"
               else s"files/$id/prov-${
                 if (inJsonFormat) "json"
                 else "freeform"
               }"

    if (inJsonFormat) postJson(path)(201)(prov)
    else postText(path)(201)(prov)
  }
}
