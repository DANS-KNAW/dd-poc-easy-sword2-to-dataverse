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

import java.net.URI
import java.nio.file.Path

import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import scalaj.http.Http

import scala.util.Try

class BagValidator(serviceUri: URI) extends DebugEnhancedLogging {
  def checkConnection(): Try[Unit] = {
    logger.info("Checking if validator service can be reached")
    Try {
      Http(s"$serviceUri")
        // TODO: Make timeouts configurable
        .timeout(connTimeoutMs = 10000, readTimeoutMs = 10000)
        .method("GET")
        .header("Accept", "text/plain")
        .asString
    } map {
      case r if r.code == 200 =>
        logger.info("OK: validator service is reachable.")
        ()
      case _ => throw new RuntimeException("Connection to Validate DANS Bag Service could not be established")
    }
  }

  def validateBag(bagDir: Path): Try[DansBagValidationResult] = {
    trace(bagDir)
    Try {
      val validationUri = serviceUri.resolve("validate?infoPackageType=SIP&uri=${bagDir.toUri}")
      logger.info(s"Calling Dans Bag Validation Service with ${ validationUri.toASCIIString }")
      Http(s"${ validationUri.toASCIIString }")
        // TODO: Make timeouts configurable
        .timeout(connTimeoutMs = 10000, readTimeoutMs = 10000)
        .method("POST")
        .header("Accept", "application/json")
        .asString
    } flatMap {
      case r if r.code == 200 =>
        DansBagValidationResult.fromJson(r.body)
      case r =>
        throw new RuntimeException(s"DANS Bag Validation failed (${ r.code }): ${ r.body }")
    }
  }
}
