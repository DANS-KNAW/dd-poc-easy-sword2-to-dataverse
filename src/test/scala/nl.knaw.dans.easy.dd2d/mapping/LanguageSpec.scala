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
import nl.knaw.dans.easy.dd2d.mapping.Language.toCitationBlockLanguage

class LanguageSpec extends TestSupportFixture {

  "toCitationBlockLanguage" should "return English as the language name" in {
    val language = <dc:language xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="ISO639-2">eng</dc:language>
    toCitationBlockLanguage(language) shouldBe Some("English")
  }

  it should "return English also when xsi:type value ends with :ISO639-2" in {
    val language = <dc:language xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="something:ISO639-2">eng</dc:language>
    toCitationBlockLanguage(language) shouldBe Some("English")
  }

  it should "return None when type attribute is not prefixed" in {
    val language = <dc:language type="ISO639-2">eng</dc:language>
    toCitationBlockLanguage(language) shouldBe None
  }

  it should "return None when prefix in type attribute is not the correct one" in {
    val language = <dc:language xmlns:xsi="http://some.thing.else" xsi:type="ISO639-2">eng</dc:language>
    toCitationBlockLanguage(language) shouldBe None
  }

}
