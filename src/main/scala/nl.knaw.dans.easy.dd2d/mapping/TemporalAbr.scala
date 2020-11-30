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

import nl.knaw.dans.easy.dd2d.dataverse.json.{ FieldMap, JsonObject }
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.xml.Node

object TemporalAbr extends BlockArchaeologySpecific with DebugEnhancedLogging {

  val ariadneTemporalToDataversename: Map[String, TermAndUrl] = Map(
    "PALEO" -> TermAndUrl("Paleolithicum: tot 8800 vC", ABR_BASE_URL + "8d762bf7-bda3-4c07-b77f-5df51301ad3f"),
    "PALEOV" -> TermAndUrl("Paleolithicum vroeg: tot 300000 C14", ABR_BASE_URL + "91330b66-c511-4800-be87-4e262b367e97"),
    "PALEOM" -> TermAndUrl("Paleolithicum midden: 300000 - 35000 C14", ABR_BASE_URL + "6dbdb234-5c13-44c6-b549-87ca9038edf3"),
    "PALEOL" -> TermAndUrl("Paleolithicum laat: 35000 C14 - 8800 vC\t", ABR_BASE_URL + "a29e5aef-b5fc-4c3f-bfbd-85134fb1f525"),
    "PALEOLA" -> TermAndUrl("Paleolithicum laat A: 35000 - 18000 C14", ABR_BASE_URL + "141b0f09-eaba-48a2-8963-bad3ac5146e3"),
    "PALEOLB" -> TermAndUrl("Paleolithicum laat B: 18000 C14 -8800 vC", ABR_BASE_URL + "934393db-3667-44e6-887b-9a1d2d794d16"),
    "MESO" -> TermAndUrl("Mesolithicum: 8800 - 4900 vC", ABR_BASE_URL + "5142431c-0520-489b-afef-60940d9f7c50"),
    "MESOV" -> TermAndUrl("Mesolithicum vroeg: 8800 - 7100 vC", ABR_BASE_URL + "6009967f-59c9-4e25-8268-4711c3c07235"),
    "MESOM" -> TermAndUrl("Mesolithicum midden: 7100 - 6450 vC", ABR_BASE_URL + "94bffc3e-84e1-4511-8a12-ebfaaa9b9eda"),
    "MESOL" -> TermAndUrl("Mesolithicum laat: 6450 -4900 vC", ABR_BASE_URL + "bb506c03-f2ee-4076-a8b5-3bce5fa5c795"),
    "NEO" -> TermAndUrl("Neolithicum: 5300 - 2000 vC", ABR_BASE_URL + "c6134150-77f2-481b-bba6-c482e59046e8"),
    "NEOV" -> TermAndUrl("Neolithicum vroeg: 5300 - 4200 vC", ABR_BASE_URL + "b8863167-1809-4a9a-8cd6-cc8aaeafa7e6"),
    "NEOVA" -> TermAndUrl("Neolithicum vroeg A: 5300 - 4900 vC", ABR_BASE_URL + "94f541a6-6ec9-47b1-927c-9b09cf448cd7"),
    "NEOVB" -> TermAndUrl("Neolithicum vroeg B: 4900 - 4200 vC", ABR_BASE_URL + "4655bf21-5d2b-4267-a8ac-8f75c21bde8b"),
    "NEOM" -> TermAndUrl("Neolithicum midden: 4200 - 2850 vC", ABR_BASE_URL + "f2309bfe-56df-4f2f-b390-49f78018abcd"),
    "NEOMA" -> TermAndUrl("Neolithicum midden A: 4200 - 3400 vC", ABR_BASE_URL + "ab077a87-4056-4f69-a417-de8bbeb9765d"),
    "NEOMB" -> TermAndUrl("Neolithicum midden B: 3400 - 2850 vC", ABR_BASE_URL + "77d4167b-9903-4abf-99ef-e28f92f618a7"),
    "NEOL" -> TermAndUrl("Neolithicum laat: 2850 - 2000 vC", ABR_BASE_URL + "c699990c-73d3-4e47-926f-5c22adec98de"),
    "NEOLA" -> TermAndUrl("Neolithicum laat A: 2850 - 2450 vC", ABR_BASE_URL + "547f2d1a-b9e4-4e61-b040-551eecfa56b1"),
    "NEOLB" -> TermAndUrl("Neolithicum laat B: 2450 - 2000 vC ", ABR_BASE_URL + "282e4b83-22c6-4df2-84d1-ef7d68bd3d5d"),
    "BRONS" -> TermAndUrl("Bronstijd: 2000 - 800 vC", ABR_BASE_URL + "8072a357-c9aa-4cd8-b8ba-c06a8e176431"),
    "BRONSV" -> TermAndUrl("Bronstijd vroeg: 2000 - 1800 vC\t", ABR_BASE_URL + "6264b6bd-899e-4c34-a88e-03a36e1d4008"),
    "BRONSM" -> TermAndUrl("Bronstijd midden: 1800 - 1100 vC", ABR_BASE_URL + "405d4012-5845-4837-8051-2a559679c30e"),
    "BRONSMA" -> TermAndUrl("Bronstijd midden A: 1800 - 1500 vC", ABR_BASE_URL + "efbd65be-d4bd-4a74-84a9-41f9b60e77e5"),
    "BRONSMB" -> TermAndUrl("Bronstijd midden B: 1500 - 1100 vC", ABR_BASE_URL + "bb71782d-72ed-4f57-9736-424747358be5"),
    "BRONSL" -> TermAndUrl("Bronstijd laat: 1100 - 800 vC", ABR_BASE_URL + "98a715d8-cd51-4125-b1b3-4c8d489f85bf"),
    "IJZ" -> TermAndUrl("IJzertijd: 800 - 12 vC", ABR_BASE_URL + "0e341d8a-d304-40fe-8dda-dd3845bb1f7f"),
    "IJZV" -> TermAndUrl("IJzertijd vroeg: 800 - 500 vC", ABR_BASE_URL + "7955ec8c-463d-4fd1-bb83-eb8451c3bd28"),
    "IJZM" -> TermAndUrl("IJzertijd midden: 500 - 250 vC", ABR_BASE_URL + "09a34060-105b-4e11-91a7-9d3484bc318d"),
    "IJZL" -> TermAndUrl("IJzertijd laat: 250 - 12 vC", ABR_BASE_URL + "e07ff2d6-1da1-4ede-9b4b-175ba229a683"),
    "ROM" -> TermAndUrl("Romeinse tijd: 12 vC - 450 nC", ABR_BASE_URL + "5a2cef7f-1fc3-45a7-9271-cd634c748e49"),
    "ROMV" -> TermAndUrl("Romeinse tijd vroeg: 12 vC - 70 nC", ABR_BASE_URL + "2afb3dea-241c-47a7-9f5b-1ce0163a114d"),
    "ROMVA" -> TermAndUrl("Romeinse tijd vroeg A: 12 vC - 25 nC", ABR_BASE_URL + "d08febfe-53a5-4053-a332-c4372b2bfe12"),
    "ROMVB" -> TermAndUrl("Romeinse tijd vroeg B: 25 - 70 nC", ABR_BASE_URL + "fcb22b7d-2857-43e9-9795-d78e712a69f1"),
    "ROMM" -> TermAndUrl("Romeinse tijd midden: 70 - 270 nC", ABR_BASE_URL + "b4715d2d-db9b-4c5b-a842-a186e024645f"),
    "ROMMA" -> TermAndUrl("Romeinse tijd midden A: 70 - 150 nC", ABR_BASE_URL + "5b253754-ddd0-4ae0-a5bb-555176bca858"),
    "ROMMB" -> TermAndUrl("Romeinse tijd midden B: 150 - 270 nC", ABR_BASE_URL + "d5c6eaee-a772-4c62-95fe-6a793765b93a"),
    "ROML" -> TermAndUrl("Romeinse tijd laat: 270 - 450 nC", ABR_BASE_URL + "15eaccb6-0739-4571-b518-88e5c081f74c"),
    "ROMLA" -> TermAndUrl("Romeinse tijd laat A: 270 - 350 nC", ABR_BASE_URL + "138bb89b-a0d1-4ab0-bf0b-4103ded264bd"),
    "ROMLB" -> TermAndUrl("Romeinse tijd laat B: 350 - 450 nC", ABR_BASE_URL + "2c686437-b730-4304-a436-06c8916c3301"),
    "XME" -> TermAndUrl("Middeleeuwen: 450 - 1500 nC", ABR_BASE_URL + "4bf24a9f-1f7d-497e-96a4-d4a0f42d564b"),
    "VME" -> TermAndUrl("Middeleeuwen vroeg: 450 - 1050 nC", ABR_BASE_URL + "ae70e517-2662-49ea-a1be-eca52b2d218e"),
    "VMEA" -> TermAndUrl("Middeleeuwen vroeg A: 450 - 525 nC", ABR_BASE_URL + "330e7fe0-a1f7-43de-b448-d477898f6648"),
    "VMEB" -> TermAndUrl("Middeleeuwen vroeg B: 525 - 725 nC", ABR_BASE_URL + "98ffaf6a-e761-4d69-8c1c-e0d75646f3ba"),
    "VMEC" -> TermAndUrl("Middeleeuwen vroeg C: 725 - 900 nC", ABR_BASE_URL + "286fb38b-a368-45aa-97f3-8151d5bbc7f6"),
    "VMED" -> TermAndUrl("Middeleeuwen vroeg D: 900 - 1050 nC ", ABR_BASE_URL + "533f6881-7c2d-49fc-bce6-71a839558c0f"),
    "LME" -> TermAndUrl("Middeleeuwen laat: 1050 - 1500 nC", ABR_BASE_URL + "ee2993b8-96d0-43cf-a350-a055476d2707"),
    "LMEA" -> TermAndUrl("Middeleeuwen laat A: 1050 - 1250 nC", ABR_BASE_URL + "f9d59f22-0ed5-435d-bd8c-e8c3b84875d5"),
    "LMEB" -> TermAndUrl("Middeleeuwen laat B: 1250 - 1500 nC", ABR_BASE_URL + "2c559d8a-4b5e-4a53-a556-b4e2e7d7dd9d"),
    "NT" -> TermAndUrl("Nieuwe tijd: 1500 - heden", ABR_BASE_URL + "c6858173-5ca2-4319-b242-f828ec53d52d"),
    "NTA" -> TermAndUrl("Nieuwe tijd A: 1500 - 1650 nC", ABR_BASE_URL + "482e37cc-61f5-4e21-a331-e342ffadeff6"),
    "NTB" -> TermAndUrl("Nieuwe tijd B: 1650 - 1850 nC", ABR_BASE_URL + "8c50361f-9c6b-4422-a2f5-4710ee1688ec"),
    "NTC" -> TermAndUrl("Nieuwe tijd C: 1850 - heden", ABR_BASE_URL + "ab1eb6b7-ecda-407a-a4ba-aaa3b6c66ec5"),
    "XXX" -> TermAndUrl("Onbekend", ABR_BASE_URL + "3db8e14e-9ee5-4b5d-a5df-b706901468a2")
  )

  def toTemporalAbr(node: Node): Option[JsonObject] = {
    val abrTemporal = ariadneTemporalToDataversename.get(node.text).map(_.term).getOrElse("")
    abrTemporal match {
      case "" =>
        logger.error(s"Invalid controlled vocabulary term for 'Temporal (ABR Period)': $abrTemporal")
        None
      case _ =>
        val m = FieldMap()
        m.addPrimitiveField(ABR_PERIOD_VALUE, ariadneTemporalToDataversename.get(node.text).map(_.term).getOrElse("Other"))
        m.addPrimitiveField(ABR_PERIOD_VOCABULARY, "ABR-periode")
        m.addPrimitiveField(ABR_PERIOD_VOCABULARY_URL, ariadneTemporalToDataversename.get(node.text).map(_.url).getOrElse(ABR_BASE_URL))
        Some(m.toJsonObject)
    }
  }

  def isNotEmpty(node: Node): Boolean = {
    !node.text.equals("")
  }

  def isTemporalAbr(node: Node): Boolean = {
    hasXsiType(node, "ABRperiode")
  }
}
