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

import nl.knaw.dans.easy.dd2d.dataverse.json.{ FileInformation, FileMetadata }
import org.scalatest.{ FlatSpec, Matchers, OneInstancePerTest }

class FilesXmlToDataverseMapperSpec extends FlatSpec with OneInstancePerTest with Matchers {
  private val mapper = new FilesXmlToDataverseMapper()

  "files.xml with no files" should "return an empty list" in {
    val filesXml = <ddm></ddm>
    val result = mapper.extractFileInfoFromFilesXml(filesXml)
    result should have size 0
    result shouldBe a[List[_]]
  }

  "files.xml" should "be mapped to a list of FileInformation case classes" in {
    val filesXml =
        <files xmlns:dcterms="http://purl.org/dc/terms/">
            <file filepath="data/random images/image01.png">
                <dcterms:title>The first image</dcterms:title>
                <dcterms:description>This description will be archived, but not displayed anywhere in the Web-UI</dcterms:description>
                <dcterms:format>image/png</dcterms:format>
                <dcterms:created>2016-11-11</dcterms:created>
            </file>
            <file filepath="data/random images/image02.jpeg">
                <dcterms:format>image/jpeg</dcterms:format>
                <dcterms:created>2016-11-10</dcterms:created>
            </file>
            <file filepath="data/reisverslag/centaur.mpg">
                <dcterms:type>http://schema.org/VideoObject</dcterms:type>
                <dcterms:title>cemtair</dcterms:title>
                <dcterms:relation xml:lang="en">data/reisverslag/centaur.srt</dcterms:relation>
                <dcterms:relation xml:lang="nl">data/reisverslag/centaur-nederlands.srt</dcterms:relation>
                <accessibleToRights>ANONYMOUS</accessibleToRights>
            </file>
        </files>

    val result = mapper.extractFileInfoFromFilesXml(filesXml)
    result should have size 3
    result should matchPattern { case List(
    FileInformation(_, FileMetadata(Some("This description will be archived, but not displayed anywhere in the Web-UI"), Some("data/random images"), Some("false"))),
    FileInformation(_, FileMetadata(None, Some("data/random images"), Some("false"))),
    FileInformation(_, FileMetadata(None, Some("data/reisverslag"), Some("false")))) =>
    }
  }
}
