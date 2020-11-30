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
import nl.knaw.dans.easy.dd2d.mapping.AccessRights.toDefaultRestrict

class AccessRightsSpec extends TestSupportFixture {

  "toDefaultRestrict" should "return false when access rights is OPEN_ACCSS" in {
    val accessRights = <ddm:accessRights>OPEN_ACCESS</ddm:accessRights>
    toDefaultRestrict(accessRights) shouldBe false
  }

  it should "return true when access rights is OPEN_ACCESS_FOR_REGISTERED_USERS" in {
    val accessRights = <ddm:accessRights>OPEN_ACCESS_FOR_REGISTERED_USERS</ddm:accessRights>
    toDefaultRestrict(accessRights) shouldBe true
  }

  it should "return true when access rights is REQUEST_PERMISSION" in {
    val accessRights = <ddm:accessRights>REQUEST_PERMISSION</ddm:accessRights>
    toDefaultRestrict(accessRights) shouldBe true
  }

  it should "return true when access rights is NO_ACCESS" in {
    val accessRights = <ddm:accessRights>NO_ACCESS</ddm:accessRights>
    toDefaultRestrict(accessRights) shouldBe true
  }

  it should "return true when access rights is something else" in {
    val accessRights = <ddm:accessRights>SOMETHING</ddm:accessRights>
    toDefaultRestrict(accessRights) shouldBe true
  }

}
