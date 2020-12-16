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
import nl.knaw.dans.easy.dd2d.mapping.Audience.{ toBasicInformationBlockSubjectCv, toCitationBlockSubject }
import nl.knaw.dans.lib.dataverse.model.dataset.PrimitiveSingleValueField

import scala.util.Success
import scala.xml.XML

class AudienceSpec extends TestSupportFixture {

  "toCitationBlockSubject" should "map the audience code to correct subject" in {
    val audience = <ddm:audience>D16</ddm:audience>
    toCitationBlockSubject(audience) shouldBe Some("Computer and Information Science")
  }

  it should "map unknown audience codes to 'Other'" in {
    val audience = <ddm:audience>UNKNOWN</ddm:audience>
    toCitationBlockSubject(audience) shouldBe Some("Other")
  }

  "toBasicInformationBlockSubjectCv" should "return the correct Narcis classification" in {
    val audience = <ddm:audience>D16</ddm:audience>
    val narcisClassification = XML.loadFile("src/test/resources/narcis_classification.xml")
    val result = toBasicInformationBlockSubjectCv(audience, narcisClassification)
    inside(result) {
      case Some(jsonObject) =>
        jsonObject.get("subjectCvVocabularyURI") shouldBe Some(PrimitiveSingleValueField("primitive", "subjectCvVocabularyURI", false, "https://www.narcis.nl/classification/D16800"))
    }
  }
}
