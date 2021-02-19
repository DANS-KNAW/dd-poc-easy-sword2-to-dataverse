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
package nl.knaw.dans.easy

import better.files.File
import nl.knaw.dans.lib.dataverse.model.file.FileMeta
import org.apache.commons.lang.StringUtils

import scala.collection.mutable
import scala.util.{ Failure, Success, Try }

package object dd2d {
  type DepositName = String
  type Sha1Hash = String
  type DatabaseId = Int

  case class VaultMetadata(dataversePid: String, dataverseBagId: String, dataverseNbn: String, dataverseOtherId: String, dataverseOtherIdVersion: String, dataverseSwordToken: String) {

    def checkMinimumFieldsForImport(): Try[Unit] = {
      val missing = new mutable.ListBuffer[String]()

      if (StringUtils.isBlank(dataversePid)) missing.append("dataversePid")
      // TODO: has not yet been implemented in export
      //      if (StringUtils.isBlank(dataverseBagId)) missing.append("dataverseBagId")
      if (StringUtils.isBlank(dataverseNbn)) missing.append("dataverseNbn")

      if (missing.nonEmpty) Failure(new RuntimeException(s"Not enough Data Vault Metadata for import deposit, missing: ${ missing.mkString(", ") }"))
      else Success(())
    }
  }

  case class FileInfo(file: File, metadata: FileMeta)

  case class RejectedDepositException(deposit: Deposit, msg: String, cause: Throwable = null)
    extends Exception(s"Rejected ${ deposit.dir }: $msg", cause)

  case class FailedDepositException(deposit: Deposit, msg: String, cause: Throwable = null)
    extends Exception(s"Failed ${ deposit.dir }: $msg", cause)

  case class InvalidDepositException(deposit: Deposit, msg: String, cause: Throwable = null)
    extends Exception(s"Not a deposit: $msg", cause)

  case class MissingRequiredFieldException(fieldName: String)
    extends Exception(s"No value found for required field: $fieldName")

  case class NonEmptyOutboxDirException(outboxDir: String)
    extends Exception(s"Output directory: $outboxDir already contains results")

  object OutboxSubdir extends Enumeration {
    type OutboxSubdir = Value
    val PROCESSED = Value("/deposits-processed")
    val REJECTED = Value("/deposits-rejected")
    val FAILED = Value("/deposits-failed")
  }
}
