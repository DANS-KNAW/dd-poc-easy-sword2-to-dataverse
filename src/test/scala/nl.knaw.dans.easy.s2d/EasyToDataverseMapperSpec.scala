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
package nl.knaw.dans.easy.s2d

import nl.knaw.dans.easy.s2d.dataverse.json.{ CompoundField, PrimitiveFieldMultipleValues, PrimitiveFieldSingleValue }
import org.json4s.DefaultFormats
import org.scalatest.{ FlatSpec, Matchers, OneInstancePerTest }

class EasyToDataverseMapperSpec extends FlatSpec with OneInstancePerTest with Matchers {

  private implicit val format: DefaultFormats.type = DefaultFormats
  private val mapper = EasyToDataverseMapper()

  "Primitive fields single value" should "be added to metadatablocks" in {
    val ddm =
        <ddm:DDM>
            <ddm:profile>
              <ddm:created>2012-12</ddm:created>
               <ddm:accessRights>Restricted Access</ddm:accessRights>
            </ddm:profile>
            <ddm:dcmiMetadata>
               <dcterms:alternative>alternatief</dcterms:alternative>
               <dcterms:language>Dutch</dcterms:language>
               <dcterms:license>MIT</dcterms:license>
            </ddm:dcmiMetadata>
        </ddm:DDM>

    mapper.mapToPrimitiveFieldsSingleValue(ddm)
    val citationFields = mapper.citationFields
    citationFields should have size 2
    citationFields should contain(PrimitiveFieldSingleValue("alternativeTitle", false, "primitive", "alternatief"))
    citationFields should contain(PrimitiveFieldSingleValue("productionDate", false, "primitive", "2012-12"))
    citationFields should not contain (PrimitiveFieldSingleValue("dateavailable", false, "primitive", "2012-12"))
    val access_and_license = mapper.access_and_LicenceFields
    access_and_license should have size 2
    access_and_license should contain(PrimitiveFieldSingleValue("license", false, "controlledVocabulary", "MIT"))
    access_and_license should contain(PrimitiveFieldSingleValue("accessrights", false, "controlledVocabulary", "Restricted Access"))
  }

  "Primitive fields multiple values" should "be added to metadatablocks" in {
    val ddm =
       <ddm:DDM>
            <ddm:profile>
            PrimitiveFieldMultipleValues(subjectAbr,true,controlledVocabulary,List(Depot, Economie – Drenkplaats/dobbe))  <ddm:audience>Law</ddm:audience>
              <ddm:audience>Agricultural Sciences</ddm:audience>
              <ddm:audience></ddm:audience>
            </ddm:profile>
            <ddm:dcmiMetadata>
              <dcterms:language>Dutch</dcterms:language>
              <dcterms:language>Bosnian</dcterms:language>
              <dc:language>English</dc:language>
              <dc:language xsi:type='dcterms:ISO639-3'>Dutch</dc:language>
              <dc:language xsi:type='dcterms:ISO639-2'>Breton</dc:language>
              <dcterms:isFormatOf xsi:type="id-type:ARCHIS-ZAAK-IDENTIFICATIE">abc</dcterms:isFormatOf>
              <dcterms:isFormatOf xsi:type="id-type:ARCHIS-ZAAK-IDENTIFICATIE">def</dcterms:isFormatOf>
              <dcterms:subject xsi:type="abr:ABRcomplex">Depot</dcterms:subject>
              <dcterms:subject xsi:type="abr:ABRcomplex">Economie – Drenkplaats/dobbe</dcterms:subject>
              <dcterms:temporal xsi:type="abr:ABRperiode">Paleolithicum laat: 35000 C14 - 8800 vC</dcterms:temporal>
              <dcterms:temporal xsi:type="abr:ABRperiode"></dcterms:temporal>
            </ddm:dcmiMetadata>
        </ddm:DDM>

    mapper.mapToPrimitiveFieldsMultipleValues(ddm)
    val citationFields = mapper.citationFields
    citationFields should have size 2
    citationFields should contain(PrimitiveFieldMultipleValues("subject", true, "controlledVocabulary", List("Law", "Agricultural Sciences")))
    citationFields should contain(PrimitiveFieldMultipleValues("language", true, "controlledVocabulary", List("Dutch", "Bosnian", "English", "Dutch", "Breton")))
    citationFields should not contain PrimitiveFieldMultipleValues("dataSources", true, "primitive", List())
    val archaeologyFields = mapper.archaeologySpecificMetadata
    archaeologyFields should have size 3
    archaeologyFields should contain(PrimitiveFieldMultipleValues("archisZaakId", true, "primitive", List("abc", "def")))
    archaeologyFields should contain(PrimitiveFieldMultipleValues("period", true, "controlledVocabulary", List("Paleolithicum laat: 35000 C14 - 8800 vC")))
    archaeologyFields should contain(PrimitiveFieldMultipleValues("subjectAbr", true, "controlledVocabulary", List("Depot", "Economie – Drenkplaats/dobbe")))
  }

  "Creator" should "be added to the Citation metadatablock as a CompoundField" in {
    val ddm =
        <ddm:DDM>
        <dcx-dai:creatorDetails>
            <dcx-dai:author>
                <dcx-dai:titles>Prof.</dcx-dai:titles>
                <dcx-dai:initials>D.N.</dcx-dai:initials>
                <dcx-dai:insertions>van den</dcx-dai:insertions>
                <dcx-dai:surname>Aarden</dcx-dai:surname>
                <dcx-dai:DAI>123456789</dcx-dai:DAI>
                <dcx-dai:organization>
                    <dcx-dai:name xml:lang="en">Utrecht University</dcx-dai:name>
                </dcx-dai:organization>
            </dcx-dai:author>
        </dcx-dai:creatorDetails>
        <dcx-dai:creator>
            <dcx-dai:author>
                <dcx-dai:titles>MSc.</dcx-dai:titles>
                <dcx-dai:initials>A.A.</dcx-dai:initials>
                <dcx-dai:insertions>van den</dcx-dai:insertions>
                <dcx-dai:surname>Aa</dcx-dai:surname>
                <dcx-dai:DAI>987654321</dcx-dai:DAI>
            </dcx-dai:author>
        </dcx-dai:creator>
        </ddm:DDM>

    mapper.addCreator(ddm)
    val citationFields = mapper.citationFields
    citationFields should have size 1
    citationFields should contain(
      CompoundField("author", true, "compound",
        List(
          Map(
            "authorAffiliation" -> PrimitiveFieldSingleValue("authorAffiliation", false, "primitive", "Utrecht University"),
            "authorName" -> PrimitiveFieldSingleValue("authorName", false, "primitive", "Prof., D.N., Aarden"),
            "authorIdentifierScheme" -> PrimitiveFieldSingleValue("authorIdentifierScheme", false, "controlledVocabulary", "DAI"),
            "authorIdentifier" -> PrimitiveFieldSingleValue("authorIdentifier", false, "primitive", "123456789")
          ),
          Map(
            "authorName" -> PrimitiveFieldSingleValue("authorName", false, "primitive", "MSc., A.A., Aa"),
            "authorIdentifierScheme" -> PrimitiveFieldSingleValue("authorIdentifierScheme", false, "controlledVocabulary", "DAI"),
            "authorIdentifier" -> PrimitiveFieldSingleValue("authorIdentifier", false, "primitive", "987654321")
          )
        )
      )
    )
  }
}
