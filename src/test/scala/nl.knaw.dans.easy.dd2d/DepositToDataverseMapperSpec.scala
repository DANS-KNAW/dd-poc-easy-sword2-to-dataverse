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

class DepositToDataverseMapperSpec extends TestSupportFixture {

  implicit val format: DefaultFormats.type = DefaultFormats
  private val mapper = new DepositToDataverseMapper
  private val vaultMetadata = Deposit(testDirValid / "valid-easy-submitted").vaultMetadata

  "toDataverseDataset" should "map profile/title to citation/title" in {
    val ddm =
      <ddm:DDM>
        <ddm:profile>
           <dc:title>A title</dc:title>
        </ddm:profile>
        <ddm:dcmiMetadata>
        </ddm:dcmiMetadata>
      </ddm:DDM>

    val result = mapper.toDataverseDataset(ddm, vaultMetadata)
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

    val result = mapper.toDataverseDataset(ddm, vaultMetadata)
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

    val result = mapper.toDataverseDataset(ddm, vaultMetadata)
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

  it should "map deposit.properties correctly to vault data" in {
    val result = mapper.toDataverseDataset(<ddm:DDM/>, vaultMetadata)
    result shouldBe a[Success[_]]
    inside(result) {
      case Success(DataverseDataset(DatasetVersion(metadataBlocks))) =>
        metadataBlocks.get("dataVault") shouldBe Some(
          MetadataBlock("Data Vault Metadata",
            List(createPrimitiveFieldSingleValue("dansDataversePid", "doi:10.17026/dans-ztg-q3s4"),
              createPrimitiveFieldSingleValue("dansNbn", "urn:nbn:nl:ui:13-ar2-u8v"),
              createPrimitiveFieldSingleValue("dansSwordToken", "sword:123e4567-e89b-12d3-a456-556642440000")))
        )
    }
  }
}