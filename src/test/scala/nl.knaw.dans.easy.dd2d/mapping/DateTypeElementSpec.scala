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
import org.json4s.native.Serialization
import org.json4s.{ DefaultFormats, Formats }

class DateTypeElementSpec extends TestSupportFixture with BlockBasicInformation {
  private implicit val jsonFormats: Formats = new DefaultFormats {}

  "toBasicInfoFormattedDateValueObject" should "create correct date details in Json object" in {
    val date = <ddm:issued>2015-08-09</ddm:issued>
    val result = Serialization.writePretty(DateTypeElement.toBasicInfoFormattedDateValueObject(date))
    findString(result, s"$DATE_TYPE.value") shouldBe "Issued"
    findString(result, s"$DATE_VALUE.value") shouldBe "2015-08-09"
  }

  it should "give 'Date' as date type" in {
    val date = <ddm:created>2015-08-09</ddm:created>
    val result = Serialization.writePretty(DateTypeElement.toBasicInfoFormattedDateValueObject(date))
    findString(result, s"$DATE_TYPE.value") shouldBe "Date"
  }

  it should "give the first day of the month when day is not given" in {
    val date = <ddm:created>2015-08</ddm:created>
    val result = Serialization.writePretty(DateTypeElement.toBasicInfoFormattedDateValueObject(date))
    findString(result, s"$DATE_VALUE.value") shouldBe "2015-08-01"
  }

  it should "give only year, month and day also when time is given" in {
    val date = <ddm:issued>2015-08-09T19:35:00.0000000Z</ddm:issued>
    val result = Serialization.writePretty(DateTypeElement.toBasicInfoFormattedDateValueObject(date))
    findString(result, s"$DATE_VALUE.value") shouldBe "2015-08-09"
  }

  "toBasicInfoFreeDateValue" should "give date in free format in Json object" in {
    val date = <ddm:issued>yhdeksäs päivä elokuuta, vuonna 2015</ddm:issued>
    val result = Serialization.writePretty(DateTypeElement.toBasicInfoFreeDateValue(date))
    findString(result, s"$DATE_FREE_FORMAT_TYPE.value") shouldBe "Issued"
    findString(result, s"$DATE_FREE_FORMAT_VALUE.value") shouldBe "yhdeksäs päivä elokuuta, vuonna 2015"
  }
}
