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
package nl.knaw.dans.easy.dd2d

import nl.knaw.dans.easy.dd2d.dataverse.json.{ CompoundField, DatasetVersion, DataverseDataset, MetadataBlock, PrimitiveFieldMultipleValues, PrimitiveFieldSingleValue, createPrimitiveFieldSingleValue }
import nl.knaw.dans.easy.dd2d.mapping.{ BlockBasicInformation, BlockCitation, BlockTemporalAndSpatial }
import org.json4s.DefaultFormats
import org.json4s.native.Serialization

import scala.util.Success

class DdmToDataverseMapperSpec extends TestSupportFixture with BlockCitation with BlockBasicInformation with BlockTemporalAndSpatial {

  implicit val format: DefaultFormats.type = DefaultFormats
  private val mapper = new DdmToDataverseMapper()

  "toDataverseDataset" should "map profile/title to citation/title" in {
    val ddm =
      <ddm:DDM>
        <ddm:profile>
           <dc:title>A title</dc:title>
        </ddm:profile>
        <ddm:dcmiMetadata>
        </ddm:dcmiMetadata>
      </ddm:DDM>

    val result = mapper.toDataverseDataset(ddm, "")
    result shouldBe a[Success[_]]
    inside(result) {
      case Success(DataverseDataset(DatasetVersion(metadataBlocks))) =>
        metadataBlocks.get("citation") shouldBe Some(
          MetadataBlock("Citation Metadata", List(createPrimitiveFieldSingleValue("title", "A title")))
        )
    }
  }

  it should "map profile/descriptions to citation/descriptions" in {
    val ddm =
      <ddm:DDM>
        <ddm:profile>
           <dc:title>A title</dc:title>
           <dc:description>Descr 1</dc:description>
           <dc:description>Descr 2</dc:description>
        </ddm:profile>
        <ddm:dcmiMetadata>
        </ddm:dcmiMetadata>
      </ddm:DDM>

    val result = mapper.toDataverseDataset(ddm, "")
    result shouldBe a[Success[_]]
    inside(result) {
      case Success(DataverseDataset(DatasetVersion(metadataBlocks))) =>
        metadataBlocks("citation").fields should contain(
          CompoundField("dsDescription",
            multiple = true,
            "compound",
            (List(
              Map("dsDescriptionValue" -> createPrimitiveFieldSingleValue("dsDescriptionValue", "Descr 1")),
              Map("dsDescriptionValue" -> createPrimitiveFieldSingleValue("dsDescriptionValue", "Descr 2"))
            )
              )))
    }
  }

  it should "map profile/creatorDetails to citation/author" in {
    val ddm =
      <ddm:DDM>
          <ddm:profile>
              <dc:title>A title</dc:title>
              <dcx-dai:creatorDetails>
                  <dcx-dai:author>
                      <dcx-dai:titles>Dr</dcx-dai:titles>
                      <dcx-dai:initials>A</dcx-dai:initials>
                      <dcx-dai:insertions>van</dcx-dai:insertions>
                      <dcx-dai:surname>Helsing</dcx-dai:surname>
                      <dcx-dai:organization>
                          <dcx-dai:name xml:lang="en">Anti-Vampire League</dcx-dai:name>
                      </dcx-dai:organization>
                  </dcx-dai:author>
              </dcx-dai:creatorDetails>
              <dcx-dai:creatorDetails>
                  <dcx-dai:author>
                      <dcx-dai:titles>Professor</dcx-dai:titles>
                      <dcx-dai:initials>T</dcx-dai:initials>
                      <dcx-dai:insertions></dcx-dai:insertions>
                      <dcx-dai:surname>Zonnebloem</dcx-dai:surname>
                      <dcx-dai:organization>
                          <dcx-dai:name xml:lang="en">Uitvindersgilde</dcx-dai:name>
                      </dcx-dai:organization>
                  </dcx-dai:author>
              </dcx-dai:creatorDetails>
          </ddm:profile>
          <ddm:dcmiMetadata>
          </ddm:dcmiMetadata>
      </ddm:DDM>

    val result = mapper.toDataverseDataset(ddm, "")
    result shouldBe a[Success[_]]
    inside(result) {
      case Success(DataverseDataset(DatasetVersion(metadataBlocks))) =>
        val valueObjectsOfCompoundFields = metadataBlocks("citation").fields.filter(_.isInstanceOf[CompoundField]).map(_.asInstanceOf[CompoundField]).flatMap(_.value)
        valueObjectsOfCompoundFields should contain(
          Map(
            "authorName" -> createPrimitiveFieldSingleValue("authorName", "Dr A van Helsing"),
            "authorAffiliation" -> createPrimitiveFieldSingleValue("authorAffiliation", "Anti-Vampire League")
          ))
        valueObjectsOfCompoundFields should contain(
          Map(
            "authorName" -> createPrimitiveFieldSingleValue("authorName", "Professor T Zonnebloem"),
            "authorAffiliation" -> createPrimitiveFieldSingleValue("authorAffiliation", "Uitvindersgilde")
          ))
    }
  }

  it should "map the whole ddm correctly to JSON objects" in {
    val ddm =
      <ddm:DDM xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" >
          <ddm:profile>
              <dc:title>A title</dc:title>
              <dc:description>Description</dc:description>
              <dc:description>Description 2</dc:description>
              <dcx-dai:creatorDetails>
                  <dcx-dai:author>
                      <dcx-dai:titles>Dr</dcx-dai:titles>
                      <dcx-dai:initials>A</dcx-dai:initials>
                      <dcx-dai:insertions>van</dcx-dai:insertions>
                      <dcx-dai:surname>Helsing</dcx-dai:surname>
                      <dcx-dai:organization>
                          <dcx-dai:name xml:lang="en">Anti-Vampire League</dcx-dai:name>
                      </dcx-dai:organization>
                  </dcx-dai:author>
              </dcx-dai:creatorDetails>
              <dcx-dai:creatorDetails>
                  <dcx-dai:author>
                      <dcx-dai:titles>Professor</dcx-dai:titles>
                      <dcx-dai:initials>T</dcx-dai:initials>
                      <dcx-dai:insertions></dcx-dai:insertions>
                      <dcx-dai:surname>Zonnebloem</dcx-dai:surname>
                      <dcx-dai:organization>
                          <dcx-dai:name xml:lang="en">Uitvindersgilde</dcx-dai:name>
                      </dcx-dai:organization>
                  </dcx-dai:author>
              </dcx-dai:creatorDetails>
              <dcx-dai:creatorDetails>
                  <dcx-dai:organization>
                      <dcx-dai:name xml:lang="en">Anti-Vampire League</dcx-dai:name>
                      <dcx-dai:role xml:lang="en">DataCurator</dcx-dai:role>
                  </dcx-dai:organization>
              </dcx-dai:creatorDetails>
              <ddm:created>2015-09-09</ddm:created>
              <ddm:available>2016-09-08</ddm:available>
              <ddm:audience>D16</ddm:audience>
              <ddm:audience>D1630</ddm:audience>
              <ddm:accessRights>NO_ACCESS</ddm:accessRights>
          </ddm:profile>
          <ddm:dcmiMetadata>
              <dcterms:isFormatOf>https://test.example/1</dcterms:isFormatOf>
              <dcterms:language xsi:type="ISO639-2">eng</dcterms:language>
              <dcterms:language xsi:type="ISO639-2">nld</dcterms:language>
              <dcterms:language>sui</dcterms:language>
              <dcterms:alternative>Alternative title</dcterms:alternative>
              <dcterms:source>source 1</dcterms:source>
              <dcterms:source>source 2</dcterms:source>
              <dcx-dai:contributorDetails>
                  <dcx-dai:author>
                      <dcx-dai:titles>Advocaat</dcx-dai:titles>
                      <dcx-dai:initials>J</dcx-dai:initials>
                      <dcx-dai:insertions></dcx-dai:insertions>
                      <dcx-dai:surname>Harker</dcx-dai:surname>
                      <dcx-dai:role xml:lang="en">Related Person</dcx-dai:role>
                  </dcx-dai:author>
              </dcx-dai:contributorDetails>
              <dcx-dai:contributorDetails>
                  <dcx-dai:organization>
                      <dcx-dai:name xml:lang="en">Advocatenkantoor Harker</dcx-dai:name>
                      <dcx-dai:role xml:lang="en">RightsHolder</dcx-dai:role>
                  </dcx-dai:organization>
              </dcx-dai:contributorDetails>
              <dcterms:hasVersion scheme="DOI">https://doi.org/10.17632/5x6xt4zhws.3</dcterms:hasVersion>
              <dcterms:references href="https://a-v.l.w.com">Anti-Vampire League website</dcterms:references>
              <dcterms:dateAccepted>2016-09-08</dcterms:dateAccepted>
              <dcterms:modified xsi:type="W3CDTF">2017-01-02</dcterms:modified>
              <dcterms:identifier xsi:type="ARCHIS-ZAAK-IDENTIFICATIE">1234567890</dcterms:identifier>
              <dcx-gml:spatial>
                  <Point xmlns="http://www.opengis.net/gml">
                      <pos>
                          52.08113 4.34510
                      </pos>
                  </Point>
              </dcx-gml:spatial>
                  <dcx-gml:spatial>
                      <boundedBy xmlns="http://www.opengis.net/gml">
                          <Envelope srsName="urn:ogc:def:crs:EPSG::28992">
                              <lowerCorner>91232.015554 436172.485680</lowerCorner>
                              <upperCorner>121811.885272 486890.494251</upperCorner>
                          </Envelope>
                      </boundedBy>
                  </dcx-gml:spatial>
          </ddm:dcmiMetadata>
      </ddm:DDM>

    val metadataBlocks = mapper.toDataverseDataset(ddm).get.datasetVersion.metadataBlocks

    // CITATION BLOCK
    val citationPrimitiveSingle = metadataBlocks("citation").fields.filter(_.isInstanceOf[PrimitiveFieldSingleValue]).map(_.asInstanceOf[PrimitiveFieldSingleValue])
    val citationPrimitiveMultiple = metadataBlocks("citation").fields.filter(_.isInstanceOf[PrimitiveFieldMultipleValues]).map(_.asInstanceOf[PrimitiveFieldMultipleValues])
    val citationCompound = metadataBlocks("citation").fields.filter(_.isInstanceOf[CompoundField]).map(_.asInstanceOf[CompoundField]).flatMap(_.value)

    val title = citationPrimitiveSingle.head.value
    val created = citationPrimitiveSingle(1).value
    val available = citationPrimitiveSingle(2).value
    val alternativeTitle = citationPrimitiveSingle(3).value
    val audience = citationPrimitiveMultiple.head.value
    val language = citationPrimitiveMultiple(1).value
    val source = citationPrimitiveMultiple(2).value
    val isFormatOf = Serialization.writePretty(citationCompound.head)
    val description1 = Serialization.writePretty(citationCompound(1))
    val description2 = Serialization.writePretty(citationCompound(2))
    val creator1 = Serialization.writePretty(citationCompound(3))
    val creator2 = Serialization.writePretty(citationCompound(4))
    val contributor = Serialization.writePretty(citationCompound(5))
    val creatorOrganization = Serialization.writePretty(citationCompound(6))
    val contributorOrganization = Serialization.writePretty(citationCompound(7))

    title shouldBe "A title"
    created shouldBe "2015-09-09"
    available shouldBe "2016-09-08"
    alternativeTitle shouldBe "Alternative title"
    audience.head shouldBe "Computer and Information Science"
    audience(1) shouldBe "Other"
    language.head shouldBe "English"
    language(1) shouldBe "Dutch"
    language.length shouldBe 2
    source.head shouldBe "source 1"
    source(1) shouldBe "source 2"
    findString(isFormatOf, s"$OTHER_ID_VALUE.value") shouldBe "https://test.example/1"
    findString(description1, s"$DESCRIPTION_VALUE.value") shouldBe "Description"
    findString(description2, s"$DESCRIPTION_VALUE.value") shouldBe "Description 2"
    findString(creator1, s"$AUTHOR_NAME.value") shouldBe "Dr A van Helsing"
    findString(creator1, s"$AUTHOR_AFFILIATION.value") shouldBe "Anti-Vampire League"
    findString(creator2, s"$AUTHOR_NAME.value") shouldBe "Professor T Zonnebloem"
    findString(creator2, s"$AUTHOR_AFFILIATION.value") shouldBe "Uitvindersgilde"
    findString(creatorOrganization, s"$CONTRIBUTOR_NAME.value") shouldBe "Anti-Vampire League"
    findString(creatorOrganization, s"$CONTRIBUTOR_TYPE.value") shouldBe "Data Curator"
    findString(contributor, s"$CONTRIBUTOR_NAME.value") shouldBe "Advocaat J Harker"
    findString(contributor, s"$CONTRIBUTOR_TYPE.value") shouldBe "Related Person"
    findString(contributorOrganization, s"$CONTRIBUTOR_NAME.value") shouldBe "Advocatenkantoor Harker"
    findString(contributorOrganization, s"$CONTRIBUTOR_TYPE.value") shouldBe "Rights Holder"

    // BASIC INFORMATION BLOCK
    val basicInfoPrimitiveMultiple = metadataBlocks("basicInformation").fields.filter(_.isInstanceOf[PrimitiveFieldMultipleValues]).map(_.asInstanceOf[PrimitiveFieldMultipleValues])
    val basicInfoCompound = metadataBlocks("basicInformation").fields.filter(_.isInstanceOf[CompoundField]).map(_.asInstanceOf[CompoundField]).flatMap(_.value)

    val languageFiles = basicInfoPrimitiveMultiple.head.value
    val relatedHasVersion = Serialization.writePretty(basicInfoCompound.head)
    val relatedIsFormatOf = Serialization.writePretty(basicInfoCompound(1))
    val relatedReferences = Serialization.writePretty(basicInfoCompound(2))
    val modified = Serialization.writePretty(basicInfoCompound(3))
    val accepted = Serialization.writePretty(basicInfoCompound(4))

    languageFiles.head shouldBe "eng"
    languageFiles(1) shouldBe "nld"
    languageFiles(2) shouldBe "sui"
    findString(relatedHasVersion, s"$RELATED_ID_IDENTIFIER.value") shouldBe "https://doi.org/10.17632/5x6xt4zhws.3"
    findString(relatedHasVersion, s"$RELATED_ID_RELATION_TYPE.value") shouldBe "has version"
    findString(relatedHasVersion, s"$RELATED_ID_SCHEME.value") shouldBe "doi"
    findString(relatedIsFormatOf, s"$RELATED_ID_URL_URL.value") shouldBe ""
    findString(relatedIsFormatOf, s"$RELATED_ID_URL_TITLE.value") shouldBe "https://test.example/1"
    findString(relatedIsFormatOf, s"$RELATED_ID_URL_RELATION_TYPE.value") shouldBe "is format of"
    findString(relatedReferences, s"$RELATED_ID_URL_URL.value") shouldBe "https://a-v.l.w.com"
    findString(relatedReferences, s"$RELATED_ID_URL_TITLE.value") shouldBe "Anti-Vampire League website"
    findString(relatedReferences, s"$RELATED_ID_URL_RELATION_TYPE.value") shouldBe "references"
    findString(relatedReferences, s"$RELATED_ID_URL_RELATION_TYPE.value") shouldBe "references"
    findString(modified, s"$DATE_TYPE.value") shouldBe "Modified"
    findString(modified, s"$DATE_VALUE.value") shouldBe "2017-01-02"
    findString(accepted, s"$DATE_FREE_FORMAT_TYPE.value") shouldBe "Date accepted"
    findString(accepted, s"$DATE_FREE_FORMAT_VALUE.value") shouldBe "2016-09-08"

    // ARCHAEOLOGY SPECIFIC BLOCK
    val archaeologyPrimitiveMultiple = metadataBlocks("archaeologyMetadata").fields.filter(_.isInstanceOf[PrimitiveFieldMultipleValues]).map(_.asInstanceOf[PrimitiveFieldMultipleValues])
    val archaeologyIdentifier = archaeologyPrimitiveMultiple.head.value
    archaeologyIdentifier.head shouldBe "1234567890"

    // TEMPORAL AND SPATIAL COVERAGE BLOCK
    val coverageCompound = metadataBlocks("temporal-spatial").fields.filter(_.isInstanceOf[CompoundField]).map(_.asInstanceOf[CompoundField]).flatMap(_.value)
    val point = Serialization.writePretty(coverageCompound.head)
    val box = Serialization.writePretty(coverageCompound(1))
    findString(point, s"$SPATIAL_POINT_SCHEME.value") shouldBe "latitude/longitude (m)"
    findString(point, s"$SPATIAL_POINT_X.value") shouldBe "52.08113"
    findString(point, s"$SPATIAL_POINT_Y.value") shouldBe "4.34510"
    findString(box, s"$SPATIAL_BOX_SCHEME.value") shouldBe "latitude/longitude (m)"
    findString(box, s"$SPATIAL_BOX_EAST.value") shouldBe "91232.015554"
    findString(box, s"$SPATIAL_BOX_NORTH.value") shouldBe "486890.494251"
    findString(box, s"$SPATIAL_BOX_SOUTH.value") shouldBe "436172.485680"
    findString(box, s"$SPATIAL_BOX_WEST.value") shouldBe "121811.885272"

  }
}