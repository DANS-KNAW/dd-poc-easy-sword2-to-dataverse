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
import nl.knaw.dans.lib.dataverse.DataverseInstanceConfig
import org.apache.commons.configuration.PropertiesConfiguration

import java.net.URI
import scala.xml.{ Elem, XML }

case class Configuration(version: String,
                         inboxDir: File,
                         validatorServiceUrl: URI,
                         validatorConnectionTimeoutMs: Int,
                         validatorReadTimeoutMs: Int,
                         dataverse: DataverseInstanceConfig,
                         autoPublish: Boolean,
                         publishAwaitUnlockMaxNumberOfRetries: Int,
                         publishAwaitUnlockMillisecondsBetweenRetries: Int,
                         narcisClassification: Elem
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

    val narcisClassificationPath = Seq(
      root / "opt" / "dans.knaw.nl" / "dd-dans-deposit-to-dataverse" / "narcis_classification.xml",
      home / "install" / "narcis_classification.xml")
      .find(_.exists)
      .getOrElse { throw new IllegalStateException("No Narcis Classification file found") }.canonicalPath
    val narcisClassification = XML.loadFile(narcisClassificationPath)

    new Configuration(
      version = (home / "bin" / "version").contentAsString.stripLineEnd,
      inboxDir = File(properties.getString("deposits.inbox")),
      validatorServiceUrl = new URI(properties.getString("validate-dans-bag.service-url")),
      validatorConnectionTimeoutMs = properties.getInt("validate-dans-bag.connection-timeout-ms"),
      validatorReadTimeoutMs = properties.getInt("validate-dans-bag.read-timeout-ms"),
      dataverse = DataverseInstanceConfig(
        connectionTimeout = properties.getInt("dataverse.connection-timeout-ms"),
        readTimeout = properties.getInt("dataverse.read-timeout-ms"),
        baseUrl = new URI(properties.getString("dataverse.base-url")),
        apiToken = properties.getString("dataverse.api-key"),
        apiVersion = properties.getString("dataverse.api-version"),
        unblockKey = Option(properties.getString("dataverse.admin-api-unblock-key")),
        awaitUnlockMaxNumberOfRetries = Option(properties.getInt("dataverse.await-unlock-max-retries")).getOrElse(10),
        awaitUnlockMillisecondsBetweenRetries = Option(properties.getInt("dataverse.await-unlock-wait-time-ms")).getOrElse(1000),
      ),
      autoPublish = properties.getString("deposits.auto-publish").toBoolean,
      publishAwaitUnlockMaxNumberOfRetries = properties.getInt("dataverse.publish.await-unlock-max-retries"),
      publishAwaitUnlockMillisecondsBetweenRetries = properties.getInt("dataverse.publish.await-unlock-wait-time-ms"),
      narcisClassification
    )
  }
}