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

class DcxDaiAuthorSpec extends TestSupportFixture with BlockCitation {
  private implicit val jsonFormats: Formats = new DefaultFormats {}

  "toAuthorValueObject" should "create correct author details in Json object" in {
    val author =
      <dcx-dai:author>
          <dcx-dai:titles>Dr</dcx-dai:titles>
          <dcx-dai:initials>A</dcx-dai:initials>
          <dcx-dai:insertions>van</dcx-dai:insertions>
          <dcx-dai:surname>Helsing</dcx-dai:surname>
          <dcx-dai:role>Provider</dcx-dai:role>
          <dcx-dai:DAI>nfo:eu-repo/dai/nl/07193567X</dcx-dai:DAI>
          <dcx-dai:ORCID>0000-0001-6438-5123</dcx-dai:ORCID>
          <dcx-dai:ISNI>0000000396540123</dcx-dai:ISNI>
          <dcx-dai:organization>
              <dcx-dai:name xml:lang="en">Anti-Vampire League</dcx-dai:name>
          </dcx-dai:organization>
      </dcx-dai:author>

    val result = Serialization.writePretty(DcxDaiAuthor.toAuthorValueObject(author))
    findString(result, s"$AUTHOR_NAME.value") shouldBe "Dr A van Helsing"
    findString(result, s"$AUTHOR_AFFILIATION.value") shouldBe "Anti-Vampire League"
    findString(result, s"$AUTHOR_IDENTIFIER_SCHEME.value") shouldBe "ORCID"
    findString(result, s"$AUTHOR_IDENTIFIER.value") shouldBe "0000-0001-6438-5123"
  }

  "toContributorValueObject" should "create correct contributor details in Json object" in {
    val author =
      <dcx-dai:author>
          <dcx-dai:titles>Dr</dcx-dai:titles>
          <dcx-dai:initials>A</dcx-dai:initials>
          <dcx-dai:insertions>van</dcx-dai:insertions>
          <dcx-dai:surname>Helsing</dcx-dai:surname>
          <dcx-dai:role>ProjectManager</dcx-dai:role>
          <dcx-dai:organization>
              <dcx-dai:name xml:lang="en">Anti-Vampire League</dcx-dai:name>
          </dcx-dai:organization>
      </dcx-dai:author>
    val result = Serialization.writePretty(DcxDaiAuthor.toContributorValueObject(author))
    findString(result, s"$CONTRIBUTOR_NAME.value") shouldBe "Dr A van Helsing (Anti-Vampire League)"
    findString(result, s"$CONTRIBUTOR_TYPE.value") shouldBe "Project Manager"
  }

  it should "give organization name as contributor name and 'other' as contributor type" in {
    val author =
      <dcx-dai:author>
          <dcx-dai:role>Contributor</dcx-dai:role>
          <dcx-dai:organization>
              <dcx-dai:name xml:lang="en">Anti-Vampire League</dcx-dai:name>
          </dcx-dai:organization>
      </dcx-dai:author>
    val result = Serialization.writePretty(DcxDaiAuthor.toContributorValueObject(author))
    findString(result, s"$CONTRIBUTOR_NAME.value") shouldBe "Anti-Vampire League"
    findString(result, s"$CONTRIBUTOR_TYPE.value") shouldBe "Other"
  }
}
