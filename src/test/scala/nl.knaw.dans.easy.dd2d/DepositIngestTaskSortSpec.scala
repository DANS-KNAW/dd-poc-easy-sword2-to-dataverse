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
import nl.knaw.dans.easy.dd2d.dansbag.DansBagValidator
import nl.knaw.dans.easy.dd2d.dataverse.DataverseInstance
import org.json4s.{ DefaultFormats, Formats }
import org.scalamock.scalatest.MockFactory

class DepositIngestTaskSortSpec extends TestSupportFixture with MockFactory {

  private implicit val jsonFormats: Formats = new DefaultFormats {}
  val depositSorter = new DepositSorter
  val dansBagValidator: DansBagValidator = mock[DansBagValidator]
  val dataverseInstance: DataverseInstance = mock[DataverseInstance]
  val depositDirs: Seq[File] = testDirUnorderedDeposits.list(_.isDirectory, maxDepth = 1).filterNot(_ == testDirUnorderedDeposits).toList
  val taskList: List[DepositIngestTask] = depositDirs.map(createDepositIngestTask).toList
  val DATASETVERSIONS_WITHOUT_FIRST_VERSION = 1

  "The result" should "be a list with size" + (depositDirs.size - DATASETVERSIONS_WITHOUT_FIRST_VERSION) in {
    val result = depositSorter.sort(taskList)
    result should have size depositDirs.size - DATASETVERSIONS_WITHOUT_FIRST_VERSION
  }

  it should "be a correctly ordered list" in {
    val result = depositSorter.sort(taskList)
    result.map(_.getTarget.dir.name) should be(List("deposit3_first", "deposit3_a", "deposit3_b", "deposit1_first", "deposit1_a", "deposit1_b", "deposit2_first", "deposit2_a"))
  }

  private def createDepositIngestTask(directory: File): DepositIngestTask = {
    DepositIngestTask(Deposit(directory), dansBagValidator, dataverseInstance, publish = false)
  }
}
