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

import better.files.File
import gov.loc.repository.bagit.domain.Bag
import gov.loc.repository.bagit.hash.StandardSupportedAlgorithms
import gov.loc.repository.bagit.reader.BagReader
import nl.knaw.dans.easy.dd2d.mapping.{ AccessRights, FileElement }
import nl.knaw.dans.lib.error.TraversableTryExtensions
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.apache.commons.configuration.PropertiesConfiguration

import java.nio.file.{ Path, Paths }
import scala.collection.JavaConverters.{ asScalaSetConverter, mapAsScalaMapConverter }
import scala.util.{ Failure, Try }
import scala.xml.{ Node, Utility, XML }

/**
 * Represents a deposit directory and provides access to the files and metadata in it.
 *
 * @param dir the deposit directory
 */
case class Deposit(dir: File) extends DebugEnhancedLogging {
  trace(dir)
  val bagDir: File = {
    checkCondition(_.isDirectory, s"$dir is not a directory")
    checkCondition(_.list.count(_.isDirectory) == 1, s"$dir has more or fewer than one subdirectory")
    checkCondition(_.list.exists(_.name == "deposit.properties"), s"$dir does not contain a deposit.properties file")
    checkCondition(_.list.filter(_.isDirectory).toList.head.list.exists(_.name == "bagit.txt"), s"$dir does not contain a bag")
    val dirs = dir.list(_.isDirectory, maxDepth = 1).filter(_ != dir).toList
    dirs.head
  }
  debug(s"bagDir = $bagDir")

  private val bagReader = new BagReader()
  private val ddmPath = bagDir / "metadata" / "dataset.xml"
  private val filesXmlPath = bagDir / "metadata" / "files.xml"
  private val agreementsXmlPath = bagDir / "metadata" / "depositor-info" / "agreements.xml"
  private val depositProperties = new PropertiesConfiguration() {
    setDelimiterParsingDisabled(true)
    load((dir / "deposit.properties").toJava)
  }

  lazy val tryBag: Try[Bag] = Try { bagReader.read(bagDir.path) }

  lazy val tryDdm: Try[Node] = Try {
    XML.loadFile((bagDir / ddmPath.toString).toJava)
  }.recoverWith {
    case t: Throwable => Failure(new IllegalArgumentException(s"Unparseable XML: ${ t.getMessage }"))
  }

  lazy val tryFilesXml: Try[Node] = Try {
    Utility.trim {
      XML.loadFile((bagDir / filesXmlPath.toString).toJava)
    }
  }.recoverWith {
    case t: Throwable => Failure(new IllegalArgumentException(s"Unparseable XML: ${ t.getMessage }"))
  }

  lazy val tryOptAgreementsXml: Try[Option[Node]] = Try {
    val agreementsFile = bagDir / agreementsXmlPath.toString
    if (agreementsFile.exists) {
      Option(Utility.trim {
        XML.loadFile((bagDir / agreementsXmlPath.toString).toJava)
      })
    }
    else {
      Option.empty[Node]
    }
  }.recoverWith {
    case t: Throwable => Failure(new IllegalArgumentException(s"Unparseable XML: ${ t.getMessage }"))
  }

  lazy val tryFilePathToSha1: Try[Map[Path, String]] = {
    for {
      bag <- tryBag
      optSha1Manifest = bag.getPayLoadManifests.asScala.find(_.getAlgorithm == StandardSupportedAlgorithms.SHA1)
      _ = if (optSha1Manifest.isEmpty) throw new IllegalArgumentException("Deposit bag does not have SHA-1 payload manifest")
      result = optSha1Manifest.get.getFileToChecksumMap.asScala.map { case (p, c) => (bagDir.path relativize p, c) }.toMap
    } yield result
  }

  def doi: String = {
    depositProperties.getString("identifier.doi", "")
  }

  def isUpdate: Try[Boolean] = {
    for {
      bag <- tryBag
      isVersionOf = bag.getMetadata.get("Is-Version-Of")
    } yield isVersionOf != null && isVersionOf.size() > 0
  }

  def getPathToFileInfo: Try[Map[Path, FileInfo]] = {
    import scala.language.postfixOps
    for {
      filesXml <- tryFilesXml
      ddm <- tryDdm
      defaultRestrict = (ddm \ "profile" \ "accessRights").headOption.forall(AccessRights toDefaultRestrict)
      files <- toFileInfos(filesXml, defaultRestrict)
    } yield files
  }

  def toFileInfos(node: Node, defaultRestrict: Boolean): Try[Map[Path, FileInfo]] = {
    (node \ "file").map(n => {
      for {
        pathToSha1 <- tryFilePathToSha1
        path = getFilePath(n)
      } yield (path, FileInfo(getFile(n), pathToSha1(path), FileElement.toFileMeta(n, defaultRestrict)))
    }).collectResults.map(_.toMap)
  }

  private def getFilePath(node: Node): Path = {
    Paths.get(node.attribute("filepath").flatMap(_.headOption).getOrElse { throw new RuntimeException("File node without a filepath attribute") }.text)
  }

  private def getFile(node: Node): File = {
    bagDir / getFilePath(node).toString
  }

  def vaultMetadata: VaultMetadata = {
    VaultMetadata(dataversePid, dataverseBagId, dataverseNbn, dataverseOtherId, dataverseOtherIdVersion, dataverseSwordToken)
  }

  def dataversePid: String = {
    dataverseIdProtocol + ":" + dataverseIdAuthority + "/" + dataverseId
  }

  private def dataverseIdProtocol: String = {
    depositProperties.getString("dataverse.id-protocol", "")
  }

  private def dataverseIdAuthority: String = {
    depositProperties.getString("dataverse.id-authority", "")
  }

  private def dataverseId: String = {
    depositProperties.getString("dataverse.id-identifier", "")
  }

  private def dataverseBagId: String = {
    depositProperties.getString("dataverse.bag-id", "")
  }

  private def dataverseNbn: String = {
    depositProperties.getString("dataverse.nbn", "")
  }

  private def dataverseOtherId: String = {
    depositProperties.getString("dataverse.other-id", "")
  }

  private def dataverseOtherIdVersion: String = {
    depositProperties.getString("dataverse.other-id-version", "")
  }

  private def dataverseSwordToken: String = {
    depositProperties.getString("dataverse.sword-token", "")
  }

  private def checkCondition(check: File => Boolean, msg: String): Unit = {
    if (!check(dir)) throw InvalidDepositException(this, msg)
  }

  override def toString: String = s"Deposit at $dir"
}
