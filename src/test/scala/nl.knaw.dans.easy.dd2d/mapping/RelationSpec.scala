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

import scala.util.{ Failure, Try }

class RelationSpec extends TestSupportFixture with BlockBasicInformation {
  private implicit val jsonFormats: Formats = new DefaultFormats {}

  "toRelatedIdentifierValueObject" should "create correct relation details in Json object" in {
    val relation = <dcterms:hasVersion scheme="DOI">https://doi.org/10.17632/5x6xt4zhws.3</dcterms:hasVersion>
    val result = Serialization.writePretty(Relation.toRelatedIdentifierValueObject(relation))
    findString(result, s"$RELATED_ID_RELATION_TYPE.value") shouldBe "has version"
    findString(result, s"$RELATED_ID_SCHEME.value") shouldBe "doi"
    findString(result, s"$RELATED_ID_IDENTIFIER.value") shouldBe "https://doi.org/10.17632/5x6xt4zhws.3"
  }

  it should "give 'relation' as relation type" in {
    val relation = <dcterms:isReferencedBy scheme="URN">urn:nbn:nl:ui:13-mia-hs1</dcterms:isReferencedBy>
    val result = Serialization.writePretty(Relation.toRelatedIdentifierValueObject(relation))
    findString(result, s"$RELATED_ID_RELATION_TYPE.value") shouldBe "relation"
    findString(result, s"$RELATED_ID_SCHEME.value") shouldBe "urn:nbn:nl"
    findString(result, s"$RELATED_ID_IDENTIFIER.value") shouldBe "urn:nbn:nl:ui:13-mia-hs1"
  }

  it should "throw exception when unknown scheme attribute is given" in {
    val relation = <dcterms:isReferencedBy scheme="urn">urn:nbn:nl:ui:13-mia-hs1</dcterms:isReferencedBy>
    inside(Try(Relation.toRelatedIdentifierValueObject(relation))) {
      case Failure(e: NoSuchElementException) => e.getMessage should include("key not found: urn")
    }
  }

  it should "throw exception when no scheme attribute is given" in {
    val relation = <dcterms:isReferencedBy>urn:nbn:nl:ui:13-mia-hs1</dcterms:isReferencedBy>
    inside(Try(Relation.toRelatedIdentifierValueObject(relation))) {
      case Failure(e: RuntimeException) => e.getMessage should include("Expected related identifier scheme")
    }
  }

  "toRelatedUrlValueObject" should "create correct relation details in Json object" in {
    val relation = <dcterms:isFormatOf href="https://a-v.l.w.com">Anti-Vampire League website</dcterms:isFormatOf>
    val result = Serialization.writePretty(Relation.toRelatedUrlValueObject(relation))
    findString(result, s"$RELATED_ID_URL_RELATION_TYPE.value") shouldBe "is format of"
    findString(result, s"$RELATED_ID_URL_URL.value") shouldBe "https://a-v.l.w.com"
    findString(result, s"$RELATED_ID_URL_TITLE.value") shouldBe "Anti-Vampire League website"
  }

  it should "give empty value for related id url" in {
    val relation = <dcterms:isFormatOf>Anti-Vampire League website</dcterms:isFormatOf>
    val result = Serialization.writePretty(Relation.toRelatedUrlValueObject(relation))
    findString(result, s"$RELATED_ID_URL_URL.value") shouldBe ""
  }

  it should "throw exception when scheme attribute is given" in {
    val relation = <dcterms:isFormatOf scheme="DOI" href="https://a-v.l.w.com">Anti-Vampire League website</dcterms:isFormatOf>
    inside(Try(Relation.toRelatedUrlValueObject(relation))) {
      case Failure(e: RuntimeException) => e.getMessage should include("Expected related URL, not a related ID")
    }
  }

}
