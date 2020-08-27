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

import java.io.PrintStream
import java.net.URI

import better.files.File
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.util.{ Failure, Try }

case class Dataset(id: String, isPersistentId: Boolean, configuration: DataverseInstanceConfig)(implicit resultOutput: PrintStream) extends HttpSupport with DebugEnhancedLogging {
  protected val connectionTimeout: Int = configuration.connectionTimeout
  protected val readTimeout: Int = configuration.readTimeout
  protected val baseUrl: URI = configuration.baseUrl
  protected val apiToken: String = configuration.apiToken
  protected val apiVersion: String = configuration.apiVersion

  def view(version: Option[String] = None): Try[String] = {
    trace(())
    if (isPersistentId) get(s"datasets/:persistentId/${ version.map(v => s"versions/$v/").getOrElse("") }?persistentId=$id")
    else get(s"datasets/$id/${ version.map(v => s"versions/$v/").getOrElse("") }")
  }

  def delete(): Try[String] = {
    trace(())
    if (isPersistentId) deletePath(s"datasets/:persistentId/?persistentId=$id")
    else deletePath(s"datasets/$id")
  }

  def listVersions(): Try[String] = {
    trace(())
    if (isPersistentId) get(s"datasets/:persistentId/versions?persistentId=$id")
    else get(s"datasets/$id/versions")
  }

  def exportMetadataTo(format: String): Try[String] = {
    trace(format)
    if (isPersistentId) get(s"datasets/export?persistentId=$id&exporter=$format", formatResponseAsJson = false)
    else Failure(CommandFailedException(501, "Export to metadata is only supported using persistent identifiers. Use the -p option", null))
  }

  def listFiles(version: Option[String] = None): Try[String] = {
    trace(version)
    if (isPersistentId) get(s"datasets/:persistentId/${ version.map(v => s"versions/$v/").getOrElse("") }files?persistentId=$id")
    else get(s"datasets/$id/${ version.map(v => s"versions/$v/").getOrElse("") }/files")
  }

  def listMetadataBlocks(version: Option[String] = None, name: Option[String]): Try[String] = {
    trace((version))
    if (isPersistentId) get(s"datasets/:persistentId/${ version.map(v => s"versions/$v/").getOrElse("") }metadata/${ name.getOrElse("") }?persistentId=$id")
    else get(s"datasets/$id/${ version.map(v => s"versions/$v/").getOrElse("") }/metadata/${ name.getOrElse("") }")
  }

  def updateMetadata(json: File, version: Option[String] = None): Try[String] = {
    trace(json, version)
    val path = if (isPersistentId) s"datasets/:persistentId/${ version.map(v => s"versions/$v/").getOrElse("") }?persistentId=$id"
               else s"datasets/$id/${ version.map(v => s"versions/$v/").getOrElse("") }/"
    tryReadFileToString(json).flatMap(put(path))
  }

  def editMetadata(json: File, replace: Boolean = false): Try[String] = {
    trace(json, replace)
    val maybeArg = if (replace) Some("replace=true")
                   else None
    tryReadFileToString(json).flatMap(put(path("editMetadata", maybeArg)))
  }

  def deleteMetadata(json: File): Try[String] = {
    trace(json)
    tryReadFileToString(json).flatMap(put(path("deleteMetadata")))
  }

  def publish(updateType: String): Try[String] = {
    trace(updateType)
    val p = path("actions/:publish", Some(s"type=$updateType"))
    postJson(p)(200, 202)(null)
  }

  private def path(action: String, maybeArg: Option[String] = None) = {
    (isPersistentId, maybeArg) match {
      case (true, None) => s"datasets/:persistentId/$action/?persistentId=$id"
      case (true, Some(arg)) => s"datasets/:persistentId/$action/?persistentId=$id&$arg"
      case (false, Some(arg)) => s"datasets/$id/$action/?$arg"
      case (false, None) => s"datasets/$id/$action/"
    }
  }

  def deleteDraft(): Try[String] = {
    trace(())
    val path = if (isPersistentId) s"datasets/:persistentId/versions/:draft/?persistentId=$id"
               else s"datasets/$id/versions/:draft/"
    deletePath(path)
  }

  def setCitationDateField(fieldName: String): Try[String] = {
    trace(())
    val path = if (isPersistentId) s"datasets/:persistentId/citationdate?persistentId=$id"
               else s"datasets/$id/citationdate"
    put(path)(s"$fieldName")
  }

  def revertCitationDateField(): Try[String] = {
    trace(())
    val path = if (isPersistentId) s"datasets/:persistentId/citationdate?persistentId=$id"
               else s"datasets/$id/citationdate"
    deletePath(path)
  }

  def listRoleAssignments(): Try[String] = {
    trace(())
    val path = if (isPersistentId) s"datasets/:persistentId/assignments?persistentId=$id"
               else s"datasets/$id/assignments"
    get(path)
  }

  def createPrivateUrl(): Try[String] = {
    trace(())
    val path = if (isPersistentId) s"datasets/:persistentId/privateUrl?persistentId=$id"
               else s"datasets/$id/privateUrl"
    postJson(path)(201)(null)
  }

  def getPrivateUrl: Try[String] = {
    trace(())
    val path = if (isPersistentId) s"datasets/:persistentId/privateUrl?persistentId=$id"
               else s"datasets/$id/privateUrl"
    get(path)
  }

  def deletePrivateUrl(): Try[String] = {
    trace(())
    val path = if (isPersistentId) s"datasets/:persistentId/privateUrl?persistentId=$id"
               else s"datasets/$id/privateUrl"
    deletePath(path)
  }

  def addFile(dataFile: File, jsonMetadata: Option[File], jsonString: Option[String]): Try[String] = {
    trace(dataFile, jsonMetadata, jsonString)
    val path = if (isPersistentId) s"datasets/:persistentId/add?persistentId=$id"
               else s"datasets/$id/add"
    jsonMetadata.map(f =>
      tryReadFileToString(f).flatMap(s =>
        postFile(path, dataFile, Some(s))(200, formatResponseAsJson = true)
      )
    ).getOrElse {
      postFile(path, dataFile, jsonString)(200, formatResponseAsJson = true)
    }
  }

  def submitForReview(): Try[String] = {
    trace(())
    val path = if (isPersistentId) s"datasets/:persistentId/submitForReview?persistentId=$id"
               else s"datasets/$id/submitForReview"
    postJson(path)(200)(null)
  }

  def returnToAuthor(reason: String): Try[String] = {
    trace(reason)
    val path = if (isPersistentId) s"datasets/:persistentId/returnToAuthor?persistentId=$id"
               else s"datasets/$id/returnToAuthor"
    postJson(path)(200)(s"""{"reasonForReturn": "$reason"}""")
  }

  def link(dataverseAlias: String): Try[String] = {
    trace(dataverseAlias)
    val path = if (isPersistentId) s"datasets/:persistentId/link/$dataverseAlias?persistentId=$id"
               else s"datasets/$id/link/$dataverseAlias"
    put(path)()
  }

  def getLocks(lockType: Option[String] = None): Try[String] = {
    trace(lockType)
    val path = if (isPersistentId) s"datasets/:persistentId/locks?persistentId=$id${ lockType.map(t => "&type=$t").getOrElse("") }"
               else s"datasets/$id/locks${ lockType.map(t => "?type=$t").getOrElse("") }"
    get(path)
  }
}
