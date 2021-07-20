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
import better.files.File.root
import nl.knaw.dans.easy.dd2d.migrationinfo.MigrationInfoConfig
import nl.knaw.dans.lib.dataverse.DataverseInstanceConfig
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.commons.csv.{ CSVFormat, CSVParser }

import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.{ Path, Paths }
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.util.Try
import scala.xml.{ Elem, XML }

case class Configuration(version: String,
                         inboxDir: File,
                         outboxDir: Path,
                         validatorServiceUrl: URI,
                         validatorConnectionTimeoutMs: Int,
                         validatorReadTimeoutMs: Int,
                         dataverse: DataverseInstanceConfig,
                         migrationInfo: MigrationInfoConfig,
                         publishAwaitUnlockMaxNumberOfRetries: Int,
                         publishAwaitUnlockMillisecondsBetweenRetries: Int,
                         narcisClassification: Elem,
                         iso1ToDataverseLanguage: Map[String, String],
                         iso2ToDataverseLanguage: Map[String, String],
                         reportIdToTerm: Map[String, String]
                        )

object Configuration {

  def apply(home: File): Configuration = {
    val cfgPath = Seq(
      root / "etc" / "opt" / "dans.knaw.nl" / "dd-dans-deposit-to-dataverse",
      home / "cfg")
      .find(_.exists)
      .getOrElse { throw new IllegalStateException("No configuration directory found") }
    val properties = new PropertiesConfiguration() {
      setDelimiterParsingDisabled(true)
      load((cfgPath / "application.properties").toJava)
    }

    def findFileInInstall(name: String): File = {
      Seq(
        root / "opt" / "dans.knaw.nl" / "dd-dans-deposit-to-dataverse" / "install" / name,
        home / "install" / name)
        .find(_.exists)
        .getOrElse { throw new IllegalStateException(s"File $name not found in APPHOME/install directory") }
    }

    val narcisClassificationFile = findFileInInstall("narcis_classification.xml")
    val narcisClassification = XML.loadFile(narcisClassificationFile.toJava)
    val iso1ToDataverseLanguageMappingFile = findFileInInstall("iso639-1-to-dv.csv")
    val iso2ToDataverseLanguageMappingFile = findFileInInstall("iso639-2-to-dv.csv")
    val rapportIdToTermMappingFile = findFileInInstall("ABR-reports.csv")

    new Configuration(
      version = (home / "bin" / "version").contentAsString.stripLineEnd,
      inboxDir = File(properties.getString("deposits.inbox")),
      outboxDir = Paths.get(properties.getString("deposits.outbox")),
      validatorServiceUrl = new URI(properties.getString("validate-dans-bag.service-url")),
      validatorConnectionTimeoutMs = properties.getInt("validate-dans-bag.connection-timeout-ms"),
      validatorReadTimeoutMs = properties.getInt("validate-dans-bag.read-timeout-ms"),
      dataverse = DataverseInstanceConfig(
        connectionTimeout = properties.getInt("dataverse.connection-timeout-ms"),
        readTimeout = properties.getInt("dataverse.read-timeout-ms"),
        baseUrl = new URI(appendSlash(properties.getString("dataverse.base-url"))),
        apiToken = properties.getString("dataverse.api-key"),
        apiVersion = properties.getString("dataverse.api-version"),
        unblockKey = Option(properties.getString("dataverse.admin-api-unblock-key")),
        awaitLockStateMaxNumberOfRetries = Option(properties.getInt("dataverse.await-unlock-max-retries")).getOrElse(10),
        awaitLockStateMillisecondsBetweenRetries = Option(properties.getInt("dataverse.await-unlock-wait-time-ms")).getOrElse(1000),
      ),
      migrationInfo = MigrationInfoConfig(
        baseUrl = new URI(appendSlash(properties.getString("migration-info.base-url"))),
        connectionTimeout = properties.getInt("migration-info.connection-timeout-ms"),
        readTimeout = properties.getInt("migration-info.read-timeout-ms")),
      publishAwaitUnlockMaxNumberOfRetries = properties.getInt("dataverse.publish.await-unlock-max-retries"),
      publishAwaitUnlockMillisecondsBetweenRetries = properties.getInt("dataverse.publish.await-unlock-wait-time-ms"),
      narcisClassification,
      iso1ToDataverseLanguage = loadCsvToMap(iso1ToDataverseLanguageMappingFile, keyColumn = "ISO639-1", valueColumn = "Dataverse-language").get,
      iso2ToDataverseLanguage = loadCsvToMap(iso2ToDataverseLanguageMappingFile, keyColumn = "ISO639-2", valueColumn = "Dataverse-language").get,
      reportIdToTerm = loadCsvToMap(rapportIdToTermMappingFile, keyColumn = "URI-suffix", valueColumn = "Term").get
    )
  }

  private def appendSlash(url: String): String = {
    if (url.endsWith("/")) url
    else url + "/"
  }

  def loadCsvToMap(csvFile: File, keyColumn: String, valueColumn: String): Try[Map[String, String]] = {
    import resource.managed

    def csvParse(csvParser: CSVParser): Map[String, String] = {
      csvParser.iterator().asScala
        .map { r => (r.get(keyColumn), r.get(valueColumn)) }.toMap
    }

    managed(CSVParser.parse(
      csvFile.toJava,
      StandardCharsets.UTF_8,
      CSVFormat.RFC4180.withFirstRecordAsHeader())).map(csvParse).tried
  }
}