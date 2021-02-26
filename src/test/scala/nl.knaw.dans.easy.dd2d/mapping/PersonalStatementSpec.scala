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
package nl.knaw.dans.easy.dd2d.mapping

import nl.knaw.dans.easy.dd2d.TestSupportFixture

class PersonalStatementSpec extends TestSupportFixture {

  "toHasPersonalData" should "return No if <containsPrivacySensitiveData> contains false" in {
    val statement = <personalDataStatement>
        <signerId easy-account="user001" email="info@dans.knaw.nl">MisterX</signerId>
        <dateSigned>2018-03-22T21:43:01.000+01:00</dateSigned>
        <containsPrivacySensitiveData>false</containsPrivacySensitiveData>
    </personalDataStatement>

    PersonalStatement.toHasPersonalDataValue(statement) shouldBe Some("No")
  }

  it should "return Yes if <containsPrivacySensitiveData> contains true" in {
    val statement = <personalDataStatement>
        <signerId easy-account="user001" email="info@dans.knaw.nl">MisterX</signerId>
        <dateSigned>2018-03-22T21:43:01.000+01:00</dateSigned>
        <containsPrivacySensitiveData>true</containsPrivacySensitiveData>
    </personalDataStatement>

    PersonalStatement.toHasPersonalDataValue(statement) shouldBe Some("Yes")
  }

  it should "return Unknown if statement contains <notAvailable/>" in {
    val statement = <personalDataStatement><notAvailable/></personalDataStatement>

    PersonalStatement.toHasPersonalDataValue(statement) shouldBe Some("Unknown")
  }

  it should "return None if statement contains no specifying element" in {
    val statement = <personalDataStatement><blah/></personalDataStatement>

    PersonalStatement.toHasPersonalDataValue(statement) shouldBe None
  }



}
