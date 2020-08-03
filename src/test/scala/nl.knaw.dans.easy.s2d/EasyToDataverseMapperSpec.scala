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

import nl.knaw.dans.easy.s2d.dataverse.json.PrimitiveFieldSingleValue
import org.json4s.DefaultFormats
import org.scalatest.{ FlatSpec, Matchers, OneInstancePerTest }

class EasyToDataverseMapperSpec extends FlatSpec with OneInstancePerTest with Matchers {

  implicit val format = DefaultFormats
  val mapper = new EasyToDataverseMapper

  "metadatablock fields" should "be updated correctly" in {
    val ddm =
        <ddm:DDM>
            <ddm:profile>
              <ddm:created>2012-12</ddm:created>
               <ddm:accessRights>Restricted Access</ddm:accessRights>
            </ddm:profile>
            <ddm:dcmiMetadata>
               <dcterms:alternative>alternatief</dcterms:alternative>
               <dcterms:language>Dutch</dcterms:language>
            </ddm:dcmiMetadata>
        </ddm:DDM>

    mapper.mapToPrimitiveFieldsSingleValue(ddm)
    assert(mapper.citationFields contains PrimitiveFieldSingleValue("alternativeTitle", false, "primitive", "alternatief"))
  }
}
