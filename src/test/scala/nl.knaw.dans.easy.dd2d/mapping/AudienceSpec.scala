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
import nl.knaw.dans.easy.dd2d.mapping.Audience.toCitationBlockSubject
import nl.knaw.dans.lib.dataverse.model.dataset.PrimitiveSingleValueField
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.xml.XML

class AudienceSpec extends TestSupportFixture with TableDrivenPropertyChecks {

  "toCitationBlockSubject" should "map the audience codes to correct subjects" in {
    val narcisAudiences = Table(
      ("audience", "subject"),
      ("D11000", "Mathematical Sciences"),
      ("D12300", "Physics"),
      ("D13200", "Chemistry"),
      ("D14320", "Engineering"),
      ("D16000", "Computer and Information Science"),
      ("D17000", "Astronomy and Astrophysics"),
      ("D18220", "Agricultural Sciences"),
      ("D22200", "Medicine, Health and Life Sciences"),
      ("D36000", "Arts and Humanities"),
      ("D41100", "Law"),
      ("D65000", "Social Sciences"),
      ("D42100", "Social Sciences"),
      ("D70100", "Business and Management"),
      ("D15300", "Earth and Environmental Sciences"),
    )

    forAll(narcisAudiences) { (audience, subject) =>
      val audienceNode = <ddm:audience>{audience}</ddm:audience>
      toCitationBlockSubject(audienceNode) shouldBe Some(subject)
    }
  }

  it should "map unknown audience codes to 'Other'" in {
    val audience = <ddm:audience>D99999</ddm:audience>
    toCitationBlockSubject(audience) shouldBe Some("Other")
  }

  it should "throw a RuntimeException for an invalid NARCIS audience code" in {
    val audience = <ddm:audience>INVALID</ddm:audience>
    assertThrows[RuntimeException] {
      toCitationBlockSubject(audience)
    }
  }
}
