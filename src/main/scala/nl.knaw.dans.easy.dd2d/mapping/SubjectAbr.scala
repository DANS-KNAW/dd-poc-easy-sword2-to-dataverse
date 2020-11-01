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

import scala.xml.Node

object SubjectAbr extends BlockArchaeologySpecific {

  val ariadneSubjectToDataversename: Map[String, TermAndUrl] = Map(
    "DEPO" -> TermAndUrl("Depot", ABR_BASE_URL + "b97ef902-059a-4c14-b925-273c74bace30"),
    "EX" -> TermAndUrl("Economie, onbepaald", ABR_BASE_URL + "a5b002d1-533c-4322-b176-1816a8a5e042"),
    "GX" -> TermAndUrl("Begraving, onbepaald", ABR_BASE_URL + "557ed9d6-ca6d-4dc4-acd3-42f454085722"),
    "IX" -> TermAndUrl("Infrastructuur, onbepaald", ABR_BASE_URL + "f53cb4cc-f7e4-47ea-be30-9d0acc14542a"),
    "NX" -> TermAndUrl("Nederzetting, onbepaald", ABR_BASE_URL + "85ae2aa0-caae-4745-aecb-6cc765a8782f"),
    "RX" -> TermAndUrl("Religie, onbepaald", ABR_BASE_URL + "4ffb1fd8-8c7b-4f55-8daa-b100c7e7fef9"),
    "VX" -> TermAndUrl("Versterking, onbepaald", ABR_BASE_URL + "99af2299-a7c5-4167-9cce-bc9efb25f5b9"),
    "XXX" -> TermAndUrl("Onbekend", ABR_BASE_URL + "39a61516-5ebd-43ad-9cde-98b5089c71ff"),
    "ELA" -> TermAndUrl("Economie – Akker/tuin", ABR_BASE_URL + "f3875055-f139-4835-9f5a-22f9d43e4f9d"),
    "EIBB" -> TermAndUrl("Economie - Beenbewerking", ABR_BASE_URL + "cdd06a0c-9f49-4bcb-ae0f-89114b486878"),
    "EIB" -> TermAndUrl("Economie - Brouwerij", ABR_BASE_URL + "e3dcc4a3-0e2b-43ae-9990-f6629c3b1535"),
    "ELCF" -> TermAndUrl("Economie - Celtic field/raatakker", ABR_BASE_URL + "a44460a9-8252-41a5-9d3e-380c86757c9a"),
    "ELDP" -> TermAndUrl("Economie - Drenkplaats/dobbe", ABR_BASE_URL + "8e3e1f10-5b97-44fb-8d68-6ec474864c9f"),
    "ELEK" -> TermAndUrl("Economie - Eendekooi", ABR_BASE_URL + "e4e24f1a-6148-4ad6-935a-b8bfa5d3b468"),
    "EIGB" -> TermAndUrl("Economie - Glasblazerij", ABR_BASE_URL + "f1c534f7-48bf-4a35-9076-adef3287197a"),
    "EGX" -> TermAndUrl("Economie - Grondstofwinning", ABR_BASE_URL + "db8e148a-9cf6-4ea8-8ab3-0a9950417dc5"),
    "EIHB" -> TermAndUrl("Economie - Houtbewerking", ABR_BASE_URL + "74e446c9-fbc8-4695-9f34-655f483ae65a"),
    "EIHK" -> TermAndUrl("Economie - Houtskool-/kolenbranderij", ABR_BASE_URL + "2aecb1f3-7f14-4e12-8bfb-d4ba9147851c"),
    "EGYW" -> TermAndUrl("Economie - IJzerwinning", ABR_BASE_URL + "1c68a564-e260-42fc-a7b3-8c760685cd13"),
    "EIX" -> TermAndUrl("Economie - Industrie/nijverheid", ABR_BASE_URL + "dccb4e11-44da-443c-939d-ef149a54d9b7"),
    "EIKB" -> TermAndUrl("Economie - Kalkbranderij", ABR_BASE_URL + "97d59086-ff33-4e2d-9749-92f3a52f7b47"),
    "EGKW" -> TermAndUrl("Economie - Kleiwinning", ABR_BASE_URL + "42e305f6-631b-4ad8-8c53-a53005de7529"),
    "ELX" -> TermAndUrl("Economie - Landbouw", ABR_BASE_URL + "033974e0-156d-4f6b-8c6b-65e3b4be5c47"),
    "EILL" -> TermAndUrl("Economie - Leerlooierij", ABR_BASE_URL + "44fff26c-c64c-489e-86d8-e89972b8aa40"),
    "EGMW" -> TermAndUrl("Economie - Mergel-/kalkwinning", ABR_BASE_URL + "35240121-7752-4567-a613-74bca32ff311"),
    "EIMB" -> TermAndUrl("Economie - Metaalbewerking/smederij", ABR_BASE_URL + "810768e0-0001-4bcb-91a5-20b25443e61a"),
    "EIM" -> TermAndUrl("Economie - Molen", ABR_BASE_URL + "d3241408-54b7-4b21-9f4f-12ad485e3ec0"),
    "EIPB" -> TermAndUrl("Economie - Pottenbakkerij", ABR_BASE_URL + "de8dc6bc-8f06-453c-ab8d-18586eb0ec5f"),
    "ESCH" -> TermAndUrl("Economie - Scheepvaart", ABR_BASE_URL + "bf9b0250-aa7f-4da8-b2c4-a577649d7870"),
    "EISM" -> TermAndUrl("Economie - Smelterij", ABR_BASE_URL + "8dc4a810-8e1c-4e3c-8735-4635528272c2"),
    "EISB" -> TermAndUrl("Economie - Steen-/pannenbakkerij", ABR_BASE_URL + "e3fb98d2-9c9a-4c1b-9cae-ccdae7063e5b"),
    "EITN" -> TermAndUrl("Economie - Textielnijverheid", ABR_BASE_URL + "6d83b0c6-d379-4ba8-b822-8a7aa19edb63"),
    "ELVK" -> TermAndUrl("Economie - Veekraal/schaapskooi", ABR_BASE_URL + "e189c2bc-44f2-4af9-8bee-b05eaadcb774"),
    "EGVW" -> TermAndUrl("Economie - Veenwinning", ABR_BASE_URL + "4d8012b9-0f95-43ba-9c36-76d86758a6bf"),
    "EVX" -> TermAndUrl("Economie - Visserij", ABR_BASE_URL + "16c6f0cc-5d41-4fca-96ca-cfac46ca1754"),
    "EIVB" -> TermAndUrl("Economie - Vuursteenbewerking", ABR_BASE_URL + "f254bb6d-95ec-4e0d-ad54-ed6d09b5cc92"),
    "EGVU" -> TermAndUrl("Economie - Vuursteenwinning", ABR_BASE_URL + "a3a50eea-4b08-4e4a-913d-1f6a56ae755f"),
    "EGZW" -> TermAndUrl("Economie - Zoutwinning/moernering", ABR_BASE_URL + "a1ea369d-dd92-45ab-9727-d79cb80a9160"),
    "GC" -> TermAndUrl("Begraving - Crematiegraf", ABR_BASE_URL + "23f38c08-4c62-40f7-a3d8-085bb5f1ed95"),
    "GD" -> TermAndUrl("Begraving - Dierengraf", ABR_BASE_URL + "df2a0682-0392-40ca-a396-4b5edf8d52a8"),
    "GHC" -> TermAndUrl("Begraving - Grafheuvel, crematie", ABR_BASE_URL + "fbd0bd66-9837-4f6d-a53a-53d08458de11"),
    "GHIC" -> TermAndUrl("Begraving - Grafheuvel, gemengd", ABR_BASE_URL + "777bb1e2-fe55-4f65-850a-8d4708839074"),
    "GHI" -> TermAndUrl("Begraving - Grafheuvel, inhumatie", ABR_BASE_URL + "9970aa8b-73ae-493a-a950-895e510fc03b"),
    "GHX" -> TermAndUrl("Begraving - Grafheuvel, onbepaald", ABR_BASE_URL + "1b9d280a-7023-4ad9-8281-110b171c1007"),
    "GVC" -> TermAndUrl("Begraving - Grafveld, crematies", ABR_BASE_URL + "e97803f6-8477-493b-ad7a-e8e1865a033d"),
    "GVIC" -> TermAndUrl("Begraving - Grafveld, gemengd", ABR_BASE_URL + "2a35dccc-a050-4ec4-9912-3bdb9c63548a"),
    "GVI" -> TermAndUrl("Begraving - Grafveld, inhumaties", ABR_BASE_URL + "01f4f3f4-e33a-413a-9734-1e3f411981af"),
    "GVX" -> TermAndUrl("Begraving - Grafveld, onbepaald", ABR_BASE_URL + "4af9d348-ee15-4eec-95c8-150eef6919b8"),
    "GI" -> TermAndUrl("Begraving - Inhumatiegraf", ABR_BASE_URL + "876e0fe7-abe9-4022-9626-daa2f7444153"),
    "GVIK" -> TermAndUrl("Begraving - Kerkhof", ABR_BASE_URL + "8dec6ef7-dfad-45a3-8d79-b038b6029371"),
    "GMEG" -> TermAndUrl("Begraving - Megalietgraf", ABR_BASE_URL + "333d2fbc-6817-41e1-b9ce-7e6d756fd53b"),
    "GVIR" -> TermAndUrl("Begraving - Rijengrafveld", ABR_BASE_URL + "a9fb73ea-979e-4c50-a5b1-f7106ec1aa73"),
    "GVCU" -> TermAndUrl("Begraving - Urnenveld", ABR_BASE_URL + "7f623fa8-fad1-4178-8e2d-7dbd03eeaad2"),
    "GCV" -> TermAndUrl("Begraving - Vlakgraf, crematie", ABR_BASE_URL + "1b496321-17ed-4f52-841a-fa21aac9093f"),
    "GIV" -> TermAndUrl("Begraving - Vlakgraf, inhumatie", ABR_BASE_URL + "44e63215-8ce0-4682-a663-5b4fcbaa51d4"),
    "GXV" -> TermAndUrl("Begraving - Vlakgraf, onbepaald", ABR_BASE_URL + "8d6aedb6-3806-40f1-a805-faef60157069"),
    "IBRU" -> TermAndUrl("Infrastructuur - Brug", ABR_BASE_URL + "60077521-9d24-4521-97b8-d7d908a1f94d"),
    "IDAM" -> TermAndUrl("Infrastructuur - Dam", ABR_BASE_URL + "fb17334e-1488-471b-aaff-74cd21740318"),
    "IDIJ" -> TermAndUrl("Infrastructuur - Dijk", ABR_BASE_URL + "366d9166-992c-412a-ade4-c3e906aa4268"),
    "IDUI" -> TermAndUrl("Infrastructuur - Duiker", ABR_BASE_URL + "570880fb-b263-480d-956a-ad3836e37566"),
    "IGEM" -> TermAndUrl("Infrastructuur - Gemaal", ABR_BASE_URL + "05641fb4-94ae-4248-afdf-a284b4f57824"),
    "IHAV" -> TermAndUrl("Infrastructuur - Haven", ABR_BASE_URL + "86378d1e-d61e-46e2-a98d-706bcfa2a49b"),
    "IKAN" -> TermAndUrl("Infrastructuur - Kanaal/vaarweg", ABR_BASE_URL + "70686979-86ad-4ce6-acac-ef9fcfe33073"),
    "IPER" -> TermAndUrl("Infrastructuur - Percelering/verkaveling", ABR_BASE_URL + "bace124f-c466-4953-8d67-1cfaca631aa8"),
    "ISLU" -> TermAndUrl("Infrastructuur - Sluis", ABR_BASE_URL + "f083fba5-671e-4b76-bb1f-b2fc51623ce3"),
    "ISTE" -> TermAndUrl("Infrastructuur - Steiger", ABR_BASE_URL + "3fa0e953-710e-4e1a-847f-cc9b861f736f"),
    "IVW" -> TermAndUrl("Infrastructuur - Veenweg/veenbrug", ABR_BASE_URL + "959701d7-413c-4581-b5fe-7bcba9dbc967"),
    "IWAT" -> TermAndUrl("Infrastructuur - Waterweg (natuurlijk)", ABR_BASE_URL + "f55d1bd5-8951-497a-aa8c-2dff13aa5ec2"),
    "IWEG" -> TermAndUrl("Infrastructuur - Weg", ABR_BASE_URL + "b24007fd-4888-4a47-a6e9-7313c3f376ba"),
    "NBAS" -> TermAndUrl("Nederzetting - Basiskamp/basisnederzetting", ABR_BASE_URL + "7ab6ce7a-8f67-443f-9c90-b163d81d5cbc"),
    "NVB" -> TermAndUrl("Nederzetting - Borg/stins/versterkt huis", ABR_BASE_URL + "058a5abe-f6de-4c3a-a05e-8f9b2e997b3c"),
    "NEXT" -> TermAndUrl("Nederzetting - Extractiekamp/-nederzetting", ABR_BASE_URL + "38431ccd-cedd-46f3-ba08-dae230e7344e"),
    "NVH" -> TermAndUrl("Nederzetting - Havezathe/ridderhofstad", ABR_BASE_URL + "4e7951e6-4713-4fe4-b08d-d81c743aac2d"),
    "NHP" -> TermAndUrl("Nederzetting - Huisplaats, onverhoogd", ABR_BASE_URL + "a94a7fff-8d3d-495f-9b56-713179ff6a4a"),
    "NHT" -> TermAndUrl("Nederzetting - Huisterp", ABR_BASE_URL + "64fea0bf-f62f-4093-a327-5c10191472c4"),
    "NKD" -> TermAndUrl("Nederzetting - Kampdorp", ABR_BASE_URL + "13f4c9a5-96dd-43b2-a06b-efa2d49fdc87"),
    "NMS" -> TermAndUrl("Nederzetting - Moated site", ABR_BASE_URL + "b13ae2fb-0e25-4eb1-bb9f-b25b73cb3938"),
    "NRV" -> TermAndUrl("Nederzetting - Romeins villa(complex)", ABR_BASE_URL + "1ff31102-f8af-480b-9b10-630ed6bfaccb"),
    "NS" -> TermAndUrl("Nederzetting - Stad", ABR_BASE_URL + "ac6a2702-d360-439c-b278-ca067390502c"),
    "NT" -> TermAndUrl("Nederzetting - Terp/wierde", ABR_BASE_URL + "70707398-14d6-477a-acdb-bdca1d56d426"),
    "NWD" -> TermAndUrl("Nederzetting - Wegdorp", ABR_BASE_URL + "ccc1242f-96db-441f-b56d-399700f4202f"),
    "RCP" -> TermAndUrl("Religie - Cultusplaats/heiligdom/tempel", ABR_BASE_URL + "92b5cb57-ca49-473c-bee7-ea2afb9e0638"),
    "RKAP" -> TermAndUrl("Religie - Kapel", ABR_BASE_URL + "678d5ce1-010c-41f1-b7f0-e83492d6e249"),
    "RKER" -> TermAndUrl("Religie - Kerk", ABR_BASE_URL + "2904916e-704c-415d-8724-fd1b97a8f7e7"),
    "RKLO" -> TermAndUrl("Religie - Klooster(complex)", ABR_BASE_URL + "54f419f0-d185-4ea5-a188-57e25493a5e0"),
    "VK" -> TermAndUrl("Versterking - Kasteel", ABR_BASE_URL + "f16ee988-133a-4ddd-9068-24985d4dfba4"),
    "VLW" -> TermAndUrl("Versterking - Landweer", ABR_BASE_URL + "99ed8c5b-9e5f-4311-8e37-3484edd6e24c"),
    "VLP" -> TermAndUrl("Versterking - Legerplaats", ABR_BASE_URL + "cc4f029d-68cf-4437-bba7-980f85f62e69"),
    "VKM" -> TermAndUrl("Versterking - Motte/kasteelheuvel/vliedberg", ABR_BASE_URL + "05062c27-fe7a-4afa-837a-ae95ddf4dacc"),
    "VSCH" -> TermAndUrl("Versterking - Schans", ABR_BASE_URL + "f4a06fe5-c3d7-4800-ac1d-759dc46ae0a0"),
    "VWP" -> TermAndUrl("Versterking - Wachtpost", ABR_BASE_URL + "cfe4e909-9d6f-4125-a831-805f304b8ecd"),
    "VWB" -> TermAndUrl("Versterking - Wal-/vluchtburcht", ABR_BASE_URL + "eb2ad681-41d1-4172-b8cf-62156b147ea2"),
    "VWAL" -> TermAndUrl("Versterking - Wal/omwalling", ABR_BASE_URL + "cb67f68a-1a01-4f16-a995-f8a1eb708b2e"),
    "VKW" -> TermAndUrl("Versterking - Waterburcht", ABR_BASE_URL + "61bf7838-56b1-4270-9648-4bb8d2e393d0")
  )

  def toSubjectAbrObject(node: Node): JsonObject = {
    val m = FieldMap()
    m.addPrimitiveField(ABR_SUBJECT_VALUE, ariadneSubjectToDataversename.get(node.text).map(_.term).getOrElse("Other"))
    m.addPrimitiveField(ABR_SUBJECT_VOCABULARY, "ABR-complex")
    m.addPrimitiveField(ABR_SUBJECT_VOCABULARY_URL, ariadneSubjectToDataversename.get(node.text).map(_.url).getOrElse(ABR_BASE_URL))
    m.toJsonObject
  }

  def isNotEmpty(node: Node): Boolean = {
    !node.text.equals("")
  }

  def isAbrComplex(node: Node): Boolean = {
    hasXsiType(node, "ABRcomplex")
  }
}
