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

package object dd2d {

  type DepositName = String

  case class VaultMetadata(dataversePid: String, dataverseBagId: String, dataverseNbn: String, dataverseOtherId: String, dataverseOtherIdVersion: String, dataverseSwordToken: String)

  case class RejectedDepositException(deposit: Deposit, msg: String, cause: Throwable = null)
    extends Exception(s"Rejected ${ deposit.dir }: $msg", cause)

  case class FailedDepositException(deposit: Deposit, msg: String, cause: Throwable = null)
    extends Exception(s"Failed ${ deposit.dir }: $msg", cause)

  case class LockException(deposit: Deposit, msg: String, cause: Throwable = null)
    extends Exception(s"Failed ${ deposit.dir }: $msg", cause)

  case class InvalidDepositException(deposit: Deposit, msg: String, cause: Throwable = null)
    extends Exception(s"Not a deposit: $msg", cause)

}