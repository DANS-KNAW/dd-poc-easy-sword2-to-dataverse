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

import nl.knaw.dans.easy.dd2d.dataverse.json.DataverseFile
import org.scalatest.{ FlatSpec, Matchers }


class FileElementSpec extends FlatSpec with Matchers {

  "toFileValueObject" should "strip data/ prefix from path to get directoryLabel" in {
    val filesXml =
      <file filepath="data/this/is/the/directoryLabel/filename.txt">
      </file>
    val result = FileElement.toFileValueObject(filesXml, defaultRestrict = true)
    result shouldBe DataverseFile(directoryLabel = Some("this/is/the/directoryLabel"))
  }
}
