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

import better.files.File
import nl.knaw.dans.easy.dd2d.mapping.Language.toCitationBlockLanguage
import nl.knaw.dans.easy.dd2d.{ Configuration, TestSupportFixture }

import java.nio.file.Paths

class LanguageSpec extends TestSupportFixture {
  private val isoToDataverseLanguage = Configuration.loadIso639ToDataverseMap(File(Paths.get("src/main/assembly/dist/install/iso639-2-to-dv.csv").toAbsolutePath)).get

  "toCitationBlockLanguage" should "return English as the language name" in {
    val language = <dc:language xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="ISO639-2">eng</dc:language>
    toCitationBlockLanguage(isoToDataverseLanguage)(language) shouldBe Some("English")
  }

  it should "return Dutch as the language name" in {
    val language = <dc:language xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="ISO639-2">nld</dc:language>
    toCitationBlockLanguage(isoToDataverseLanguage)(language) shouldBe Some("Dutch")
  }

  it should "return French as the language name" in {
    val language = <dc:language xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="ISO639-2">fre</dc:language>
    toCitationBlockLanguage(isoToDataverseLanguage)(language) shouldBe Some("French")
  }

  it should "return German as the language name" in {
    val language = <dc:language xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="ISO639-2">deu</dc:language>
    toCitationBlockLanguage(isoToDataverseLanguage)(language) shouldBe Some("German")
  }

  it should "return None when type attribute is not prefixed" in {
    val language = <dc:language type="ISO639-2">eng</dc:language>
    toCitationBlockLanguage(isoToDataverseLanguage)(language) shouldBe None
  }

  it should "return None when prefix in type attribute is not the correct one" in {
    val language = <dc:language xmlns:xsi="http://some.thing.else" xsi:type="ISO639-2">eng</dc:language>
    toCitationBlockLanguage(isoToDataverseLanguage)(language) shouldBe None
  }

  it should "map languages with diacritical marks correctly" in {
    val language = <dc:language xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="something:ISO639-2">san</dc:language>
    val `Latin Small Letter M with Dot Above` = "\u1e41" // https://unicode-table.com/en/1E41/
    val `Latin Small Letter R with Dot Below` = "\u1e5b" // https://unicode-table.com/en/1E5B/

    toCitationBlockLanguage(isoToDataverseLanguage)(language) shouldBe
      Some(s"Sanskrit (Sa${ `Latin Small Letter M with Dot Above` }sk${ `Latin Small Letter R with Dot Below` }ta)")
  }

  it should "also accept the encodingScheme attribute to indicate ISO639-2" in {
    val language = <dc:language encodingScheme="ISO639-2">deu</dc:language>
    toCitationBlockLanguage(isoToDataverseLanguage)(language) shouldBe Some("German")
  }
}
