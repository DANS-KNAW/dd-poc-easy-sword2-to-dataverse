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
package nl.knaw.dans.easy.dd2d.queue

import nl.knaw.dans.easy.dd2d.TestSupportFixture
import org.scalatest.Matchers

class DepositDirListSortSpec extends TestSupportFixture with Matchers {

  val depositDirs = testDirUnorderedDeposits.list(_.isDirectory, maxDepth = 1).filterNot(_ == testDirUnorderedDeposits).toList
  val DATASETVERSIONS_WITHOUT_FIRST_VERSION = 1

  "The result" should "be a list with size" + (depositDirs.size - DATASETVERSIONS_WITHOUT_FIRST_VERSION) in {
    val result = SortUtils.sortDeposits(depositDirs)
    result should have size depositDirs.size - DATASETVERSIONS_WITHOUT_FIRST_VERSION
  }

  it should "be a correctly ordered list" in {
    val result = SortUtils.sortDeposits(depositDirs)
    result.map(_.name) should be(List("deposit2", "deposit2_a", "deposit3", "deposit3_a", "deposit3_b", "deposit1", "deposit1_a", "deposit1_b"))
  }
}
