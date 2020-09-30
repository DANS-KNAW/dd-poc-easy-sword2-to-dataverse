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

import nl.knaw.dans.easy.dd2d.dataverse.json.{ CompoundField, DatasetVersion, DataverseDataset, Field, MetadataBlock, PrimitiveFieldMultipleValues, PrimitiveFieldSingleValue, createCompoundFieldMultipleValues, createPrimitiveFieldSingleValue }
import org.apache.commons.lang.StringUtils
import org.json4s.DefaultFormats
import org.json4s.native.Serialization

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Try
import scala.xml.{ Elem, MetaData, Node, NodeSeq }

/**
 * Maps DANS Dataset Metadata to Dataverse Json
 */
class DdmToDataverseMapper() {
  private implicit val format: DefaultFormats.type = DefaultFormats
  case class RelatedIdentifier(relationType: String, schemeOrUrl: String, value: String, isRelatedIdentifier: Boolean)
  case class PointCoordinate(x: String, y: String)
  case class BoxCoordinate(north: String, south: String, east: String, west: String)
  lazy val citationFields = new ListBuffer[Field]
  lazy val accessAndLicenseFields = new ListBuffer[Field]
  lazy val depositAgreementFields = new ListBuffer[Field]
  lazy val basicInformationFields = new ListBuffer[Field]
  lazy val archaeologySpecificFields = new ListBuffer[Field]
  lazy val temporalSpatialFields = new ListBuffer[Field]
  //todo Why is absolute path not complete using better.files.File?

  object Spatial {
    /** coordinate order y, x = latitude (DCX_SPATIAL_Y), longitude (DCX_SPATIAL_X) */
    val DEGREES_SRS_NAME = "http://www.opengis.net/def/crs/EPSG/0/4326"

    /** coordinate order x, y = longitude (DCX_SPATIAL_X), latitude (DCX_SPATIAL_Y) */
    val RD_SRS_NAME = "http://www.opengis.net/def/crs/EPSG/0/28992"
  }

  object IdScheme extends Enumeration {
    type IdScheme = Value
    val DOI: IdScheme.Value = Value("doi")
    val URN: IdScheme.Value = Value("urn:nbn:nl")
    val ISBN: IdScheme.Value = Value("ISBN")
    val ISSN: IdScheme.Value = Value("ISSN")
    val NWO: IdScheme.Value = Value("NWO ProjectNr")
    val OTHER: IdScheme.Value = Value("other")
    val DEFAULT: IdScheme.Value = Value("")
  }

  def toDataverseDataset(ddm: Node): Try[DataverseDataset] = Try {
    // Profile fields
    addPrimitiveFieldSingleValue(citationFields, "title", (ddm \ "profile" \ "title")) // TODO: Conflict: is multi-value in DDM
    addCompoundFieldMultipleValues(citationFields, "dsDescription", createDescriptionValueObjectsFrom((ddm \ "profile" \ "description")))
    addCompoundFieldMultipleValues(citationFields, "author", createAuthorValueObjectsFromCreatorDetails((ddm \ "profile" \ "creatorDetails")))
    // TODO: creator unstructured
    addPrimitiveFieldSingleValue(citationFields, "productionDate", (ddm \ "profile" \ "created"))
    addPrimitiveFieldSingleValue(citationFields, "distributionDate", (ddm \ "profile" \ "available")) // TODO: correct target for available?
    addCvFieldMultipleValues(citationFields, "subject", createSubjectsFromNarcisCodes((ddm \ "profile" \ "audience")))

    // dcmiMetadata
    addPrimitiveFieldSingleValue(citationFields, "alternativeTitle", (ddm \ "dcmiMetadata" \ "alternative"))

    assembleDataverseDataset()
  }

  private def assembleDataverseDataset(): DataverseDataset = {
    val versionMap = mutable.Map[String, MetadataBlock]()
    addMetadataBlock(versionMap, "citation", "Citation Metadata", citationFields)

    val datasetVersion = DatasetVersion(versionMap.toMap)
    DataverseDataset(datasetVersion)
  }

  private def addPrimitiveFieldSingleValue(metadataBlockFields: ListBuffer[Field], name: String, elements: NodeSeq): Unit = {
    if (elements.nonEmpty) {
      metadataBlockFields += PrimitiveFieldSingleValue(name, multiple = false, "primitive", elements.map(_.text).head)
    }
  }

  def addCompoundFieldMultipleValues(metadataBlockFields: ListBuffer[Field], name: String, valueObjects: List[Map[String, Field]]): Unit = {
    if (valueObjects.nonEmpty) {
      metadataBlockFields += createCompoundFieldMultipleValues(name, valueObjects)
    }
  }

  private def addPrimitiveFieldMultipleValues(metadataBlockFields: ListBuffer[Field], name: String, elements: NodeSeq): Unit = {
    if (elements.nonEmpty) {
      metadataBlockFields += PrimitiveFieldMultipleValues(name, multiple = true, "primitive", elements.map(_.text).toList)
    }
  }

  private def addPrimitiveFieldMultipleValues(metadataBlockFields: ListBuffer[Field], name: String, values: List[String]): Unit = {
    if (values.nonEmpty) {
      metadataBlockFields += PrimitiveFieldMultipleValues(name, multiple = true, "primitive", values)
    }
  }

  private def addCvFieldMultipleValues(metadataBlockFields: ListBuffer[Field], name: String, values: List[String]): Unit = {
    if (values.nonEmpty) {
      metadataBlockFields += PrimitiveFieldMultipleValues(name, multiple = true, "controlledVocabulary", values)
    }
  }


  private def addMetadataBlock(versionMap: mutable.Map[String, MetadataBlock], blockId: String, blockDisplayName: String, fields: ListBuffer[Field]): Unit = {
    if (fields.nonEmpty) {
      versionMap.put(blockId, MetadataBlock(blockDisplayName, fields.toList))
    }
  }

  /**
   * Converts easy-ddm xml to Scala case classes which at the end
   * are converted to json using the Json4s library
   *
   * @param ddm easy-ddm
   * @return Json String
   */
  def mapToJson(ddm: Node): Try[String] = Try {
    // Todo Which title: DV allows only one
    val title = (ddm \\ "title").head.text
    citationFields += PrimitiveFieldSingleValue("title", multiple = false, "primitive", Some(title).getOrElse(""))

    mapToPrimitiveFieldsSingleValue(ddm)
    mapToPrimitiveFieldsMultipleValues(ddm)
    mapToCompoundFields(ddm)
    val citationBlock = MetadataBlock("Citation Metadata", citationFields.toList)
    var versionMap = scala.collection.mutable.Map("citation" -> citationBlock)

    if (accessAndLicenseFields.nonEmpty) {
      val accessAndLicenseBlock = MetadataBlock("Access and License", accessAndLicenseFields.toList)
      versionMap += ("access-and-license" -> accessAndLicenseBlock)
    }
    if (depositAgreementFields.nonEmpty) {
      val depositAgreementBlock = MetadataBlock("Deposit Agreement", depositAgreementFields.toList)
      versionMap += ("depositAgreement" -> depositAgreementBlock)
    }
    if (basicInformationFields.nonEmpty) {
      val basicInformation = MetadataBlock("Basic Information", basicInformationFields.toList)
      versionMap += ("basicInformation" -> basicInformation)
    }
    if (archaeologySpecificFields.nonEmpty) {
      val archaeologyMetadataBlock = MetadataBlock("archaeologyMetadata", archaeologySpecificFields.toList)
      versionMap += ("archaeologyMetadata" -> archaeologyMetadataBlock)
    }
    if (temporalSpatialFields.nonEmpty) {
      val temporalSpatialBlock = MetadataBlock("Temporal and Spatial Coverage", temporalSpatialFields.toList)
      versionMap += ("temporal-spatial" -> temporalSpatialBlock)
    }

    val datasetVersion = DatasetVersion(versionMap.toMap)
    val dataverseDataset = DataverseDataset(datasetVersion)
    Serialization.writePretty(dataverseDataset)
  }

  def mapToPrimitiveFieldsSingleValue(ddm: Node): Try[Unit] = Try {
    (ddm \\ "_").foreach {
      case e @ Elem("dcterms", "alternative", _, _, _) => citationFields += PrimitiveFieldSingleValue("alternativeTitle", multiple = false, "primitive", Some(e.text).getOrElse(""))
      //case node @ _ if node.label.equals("creatorDetails") => addCreator(node)
      case e @ Elem("ddm", "created", _, _, _) => citationFields += PrimitiveFieldSingleValue("productionDate", multiple = false, "primitive", Some(e.text).getOrElse(""))
      case e @ Elem("ddm", "accessRights", _, _, _) => accessAndLicenseFields += PrimitiveFieldSingleValue("accessrights", multiple = false, "controlledVocabulary", Some(e.text).getOrElse(""))
      case e @ Elem("dcterms", "license", _, _, _) => accessAndLicenseFields += PrimitiveFieldSingleValue("license", multiple = false, "controlledVocabulary", Some(e.text).getOrElse(""))
      //Todo get the correct element
      case e @ Elem(_, "instructionalMethod", _, _, _) => depositAgreementFields += PrimitiveFieldSingleValue("accept", multiple = false, "controlledVocabulary", Some(e.text).getOrElse(""))
      case _ => ()
    }
    //just search in "profile" because "dcmiMetadata" can also contain data available
    (ddm \ "profile").head.nonEmptyChildren.foreach {
      case e @ Elem("ddm", "available", _, _, _) => accessAndLicenseFields += PrimitiveFieldSingleValue("dateavailable", multiple = false, "primitive", Some(e.text).getOrElse(""))
      case _ => ()
    }

    val languageOfDescription: String = (ddm \\ "description")
      .headOption
      .map(_.attributes.filter(_.key == "lang"))
      .map(_.value).getOrElse("")
      .toString

    basicInformationFields += PrimitiveFieldSingleValue("languageofmetadata", multiple = false, "controlledVocabulary", Some(languageOfDescription).getOrElse(""))
  }

  def mapToPrimitiveFieldsMultipleValues(node: Node): Try[Unit] = Try {
    // Todo create mapping for EASY Dropdown values (d2700 etc.) to DV values
    val audience = (node \\ "audience").filter(e => !e.text.equals("")).map(_.text).toList
    if (audience.nonEmpty)
      citationFields += PrimitiveFieldMultipleValues("subject", multiple = true, "controlledVocabulary", Some(audience).getOrElse(List()))

    val language = (node \\ "language").filter(e => !e.text.equals("")).map(_.text).toList
    if (language.nonEmpty)
      citationFields += PrimitiveFieldMultipleValues("language", multiple = true, "controlledVocabulary", Some(language).getOrElse(List()))

    val source = (node \\ "source").filter(e => !e.text.equals("")).map(_.text).toList
    if (source.nonEmpty)
      citationFields += PrimitiveFieldMultipleValues("dataSources", multiple = true, "primitive", Some(source).getOrElse(List()))

    val zaakId = (node \\ "_").filter(_.attributes.exists(_.value.text == "id-type:ARCHIS-ZAAK-IDENTIFICATIE")).filter(e => !e.text.equals("")).toList.map(_.text)
    if (zaakId.nonEmpty)
      archaeologySpecificFields += PrimitiveFieldMultipleValues("archisZaakId", multiple = true, "primitive", Some(zaakId).getOrElse(List()))

    val abrComplex = (node \\ "_").filter(_.attributes.exists(_.value.text == "abr:ABRcomplex")).filter(e => !e.text.equals("")).toList.map(_.text)
    if (abrComplex.nonEmpty)
      archaeologySpecificFields += PrimitiveFieldMultipleValues("subjectAbr", multiple = true, "controlledVocabulary", Some(abrComplex).getOrElse(List()))

    val period = (node \\ "temporal").filter(_.attributes.exists(_.value.text == "abr:ABRperiode")).filter(e => !e.text.equals("")).toList.map(_.text)
    if (period.nonEmpty)
      archaeologySpecificFields += PrimitiveFieldMultipleValues("period", multiple = true, "controlledVocabulary", Some(period).getOrElse(List()))

    val temporalCoverage = (node \\ "temporal").filterNot(_.attributes.exists(_.value.text == "abr:ABRperiode")).filter(e => !e.text.equals("")).toList.map(_.text)
    if (temporalCoverage.nonEmpty)
      archaeologySpecificFields += PrimitiveFieldMultipleValues("temporal-coverage", multiple = true, "primitive", Some(temporalCoverage).getOrElse(List()))

    val languageOfFiles = (node \\ "language").filter(e => !e.text.equals("")).map(_.text).toList
    basicInformationFields += PrimitiveFieldMultipleValues("languageFiles", multiple = true, "primitive", Some(languageOfFiles).getOrElse(List()))
  }

  def mapToCompoundFields(node: Node): Try[Unit] = Try {
    addCreator(node)
    addContributors(node)
    addAlternativeIdentifier(node)
    addDates(node)
    addDatesFreeFormat(node)
    addRelatedIdentifiers(node)
    addPeopleAndOrganisation(node)
    addKeywords(node)
    addSpatialPoint(node)
    addSpatialBox(node)
    addDescriptions(node)
  }

  def createDescriptionValueObjectsFrom(descriptionElements: NodeSeq): List[Map[String, Field]] = {
    val valueObjects = new ListBuffer[Map[String, Field]]()
    descriptionElements
      .foreach(d => {
        valueObjects += Map(
          "dsDescriptionValue" -> createPrimitiveFieldSingleValue("dsDescriptionValue", d.text)
          // TODO: add date subfield?
        )
      })
    valueObjects.toList
  }

  def createAuthorValueObjectsFromCreatorDetails(creatorDetailsElements: NodeSeq): List[Map[String, Field]] = {
    val valueObjects = new ListBuffer[Map[String, Field]]()
    creatorDetailsElements
      .foreach(e => {
        val authorElement = (e \ "author")
        val titles = (authorElement \ "titles").map(_.text).headOption.getOrElse("")
        val initials = (authorElement \ "initials").map(_.text).headOption.getOrElse("")
        val insertions = (authorElement \ "insertions").map(_.text).headOption.getOrElse("")
        val surname = (authorElement \ "surname").map(_.text).headOption.getOrElse("")
        val organization = (authorElement \ "organization" \ "name").map(_.text).headOption.getOrElse("")
        val name = List(titles, initials, insertions, surname).mkString(" ").trim().replaceAll("\\s+", " ")
        val valueObject = mutable.Map[String, Field]()

        if (StringUtils.isNotBlank(name)) {
          valueObject.put("authorName", createPrimitiveFieldSingleValue("authorName", name))
        }
        if (StringUtils.isNotBlank(organization)) {
          valueObject.put("authorAffiliation", createPrimitiveFieldSingleValue("authorAffiliation", organization))
        }

        // TODO: Add DAI, ISNI, ORCID
        valueObjects += valueObject.toMap
      })
    valueObjects.toList
  }

  def createSubjectsFromNarcisCodes(audiences: NodeSeq): List[String] = {
    val narcisToSubject = Map(
      "D11" -> "Mathematical Sciences",
      "D12" -> "Physics",
      "D13" -> "Chemistry",
      "D14" -> "Engineering",
      "D16" -> "Computer and Information Science",
      "D17" -> "Astronomy and Astrophysics",
      "D18" -> "Agricultural Sciences",
      "D2" -> "Medicine, Health and Life Sciences",
      "D3" -> "Arts and Humanities",
      "D4" -> "Law",
      "D6" -> "Social Sciences",
      "D7" -> "Business and Management",
      "E15" -> "Earth and Environmental Sciences"
   )

    audiences.toList.map {
      a =>
        val code = a.text.take(3).takeWhile(_ != '0') // TODO: check this algorithm
        narcisToSubject.getOrElse(code, "Other")
    }
  }

  def addDescriptions(node: Node): Unit = {
    val objectList = new ListBuffer[Map[String, Field]]()
    ((node \ "profile" \ "description") ++ (node \ "dcmiMetadata" \ "description"))
      .foreach(d => {
        var subFields = collection.mutable.Map[String, Field]()
        subFields += ("dsDescriptionValue" -> PrimitiveFieldSingleValue("dsDescriptionValue", false, "primitive", d.text))
        //todo descriptionDate omitted because it doesn't exist in EASY
        //subFields += ("dsDescriptionDate" -> PrimitiveFieldSingleValue("dsDescriptionDate", false, "primitive", "NA"))
        objectList += subFields.toMap
      })
    citationFields += CompoundField("dsDescription", multiple = true, "compound", objectList.toList)
  }

  def addSpatialBox(node: Node): Unit = {
    val objectList = new ListBuffer[Map[String, Field]]()
    (node \\ "spatial").filter(x => (x \\ "lowerCorner").nonEmpty).foreach(spatial => {
      var subFields = collection.mutable.Map[String, Field]()
      val isRD = (spatial \\ "Envelope").head.attributes.exists(_.value.text.equals(Spatial.RD_SRS_NAME))

      if (isRD)
        subFields += ("easy-tsm-spatial-box" -> PrimitiveFieldSingleValue("easy-tsm-spatial-box", false, "controlledVocabulary", "RD(in m.)"))
      else
        subFields += ("easy-tsm-spatial-box" -> PrimitiveFieldSingleValue("easy-tsm-spatial-box", false, "controlledVocabulary", "latitude/longitude (m)"))

      val lowerCorner = (spatial \\ "lowerCorner")
        .find(!_.text.isEmpty)
        .map(
          _.text.split(" +")
            .take(2).toList
        ).getOrElse(List(""))
      val upperCorner = (spatial \\ "upperCorner")
        .find(!_.text.isEmpty)
        .map(
          _.text.split(" +")
            .take(2).toList
        ).getOrElse(List(""))

      val boxCoordinate = getBoxCoordinate(isRD, lowerCorner, upperCorner)
      subFields += ("easy-tsm-spatial-box-north" -> PrimitiveFieldSingleValue("easy-tsm-spatial-box-north", false, "primitive", boxCoordinate.north))
      subFields += ("easy-tsm-spatial-box-east" -> PrimitiveFieldSingleValue("easy-tsm-spatial-box-east", false, "primitive", boxCoordinate.east))
      subFields += ("easy-tsm-spatial-box-south" -> PrimitiveFieldSingleValue("easy-tsm-spatial-box-south", false, "primitive", boxCoordinate.south))
      subFields += ("easy-tsm-spatial-box-west" -> PrimitiveFieldSingleValue("easy-tsm-spatial-box-west", false, "primitive", boxCoordinate.west))
      objectList += subFields.toMap
    })
    temporalSpatialFields += CompoundField("easy-spatial-box", multiple = true, "compound", objectList.toList)
  }

  def addSpatialPoint(node: Node): Unit = {
    val objectList = new ListBuffer[Map[String, Field]]()
    (node \\ "spatial").filter(x => (x \\ "Point").nonEmpty).foreach(spatial => {
      var subFields = collection.mutable.Map[String, Field]()
      val isRD = spatial.attributes.exists(_.value.text.equals(Spatial.RD_SRS_NAME))
      if (isRD)
        subFields += ("easy-tsm-spatial-point" -> PrimitiveFieldSingleValue("easy-tsm-spatial-point", false, "controlledVocabulary", "RD(in m.)"))
      else
        subFields += ("easy-tsm-spatial-point" -> PrimitiveFieldSingleValue("easy-tsm-spatial-point", false, "controlledVocabulary", "latitude/longitude (m)"))

      val pos = (spatial \\ "pos").filter(!_.text.isEmpty).head.text.split(" +").take(2).toList
      val coordinate = getPointCoordinate(isRD, pos)
      subFields += ("easy-tsm-x" -> PrimitiveFieldSingleValue("easy-tsm-x", false, "primitive", coordinate.x))
      subFields += ("easy-tsm-y" -> PrimitiveFieldSingleValue("easy-tsm-y", false, "primitive", coordinate.y))
      objectList += subFields.toMap
    })
    temporalSpatialFields += CompoundField("easy-tsm", multiple = true, "compound", objectList.toList)
  }

  def addKeywords(node: Node): Try[Unit] = Try {
    val objectList = new ListBuffer[Map[String, Field]]()
    val keywords = node \\ "subject"
    if (keywords.nonEmpty) {
      keywords.foreach(subject => {
        var subFields = collection.mutable.Map[String, Field]()
        subFields += ("keywordValue" -> PrimitiveFieldSingleValue("keywordValue", multiple = false, "primitive", subject.text))
        //subFields += ("keywordVocabulary" -> PrimitiveFieldSingleValue("keywordVocabulary", multiple = false, "primitive", "NOT AVAILABLE IN EASY"))
        //subFields += ("keywordVocabularyURI" -> PrimitiveFieldSingleValue("keywordVocabularyURI", multiple = false, "primitive", "NOT AVAILABLE IN EASY"))
        objectList += subFields.toMap
      })
      citationFields += CompoundField("keyword", multiple = true, "compound", objectList.toList)
    }
  }

  //Todo change UI rendering order and formatting in .tsv files
  def addPeopleAndOrganisation(node: Node): Try[Unit] = Try {
    val po = node \\ "author"
    val objectList = new ListBuffer[Map[String, Field]]()
    var subFields = collection.mutable.Map[String, Field]()

    if (po.nonEmpty) {
      po.foreach(e => {
        e.head.nonEmptyChildren.foreach {
          case e @ Elem("dcx-dai", "role", _, _, _) => subFields += ("easy-pno-role" -> PrimitiveFieldSingleValue("easy-pno-role", false, "controlledVocabulary", e.text))
          case e @ Elem("dcx-dai", "titles", _, _, _) => subFields += ("easy-pno-titles" -> PrimitiveFieldSingleValue("easy-pno-titles", false, "primitive", e.text))
          case e @ Elem("dcx-dai", "initials", _, _, _) => subFields += ("easy-pno-initials" -> PrimitiveFieldSingleValue("easy-pno-initials", false, "primitive", e.text))
          case e @ Elem("dcx-dai", "insertions", _, _, _) => subFields += ("easy-pno-prefix" -> PrimitiveFieldSingleValue("easy-pno-prefix", false, "primitive", e.text))
          case e @ Elem("dcx-dai", "surname", _, _, _) => subFields += ("easy-pno-surname" -> PrimitiveFieldSingleValue("easy-pno-surname", false, "primitive", e.text))
          case node @ _ if node.label.equals("organization") => subFields += ("easy-pno-organisation" -> PrimitiveFieldSingleValue("easy-pno-organisation", false, "primitive", (node \ "name").head.text))
          case e @ Elem("dcx-dai", "ORCID", _, _, _) => subFields += ("easy-pno-id-orcid" -> PrimitiveFieldSingleValue("easy-pno-id-orcid", false, "primitive", e.text))
          case e @ Elem("dcx-dai", "ISNI", _, _, _) => subFields += ("easy-pno-id-isni" -> PrimitiveFieldSingleValue("easy-pno-id-isni", false, "primitive", e.text))
          case e @ Elem("dcx-dai", "DAI", _, _, _) => subFields += ("easy-pno-id-dai" -> PrimitiveFieldSingleValue("easy-pno-id-dai", false, "primitive", e.text))
          case _ => ()
        }
        objectList += subFields.toMap
      })
      basicInformationFields += CompoundField("easy-pno", multiple = true, "compound", objectList.toList)
    }
  }

  def addCreator(node: Node): Try[Unit] = Try {
    val creators = ((node \\ "creatorDetails") ++ (node \\ "creator")) \\ "author"
    val objectList = new ListBuffer[Map[String, Field]]()
    if (creators.nonEmpty) {
      creators.foreach(creatorNode => {
        var subFields = collection.mutable.Map[String, Field]()
        subFields += ("authorName" -> PrimitiveFieldSingleValue("authorName", multiple = false, "primitive", getAuthorName(creatorNode.head)))
        creatorNode.nonEmptyChildren.foreach {
          case node @ _ if node.label.equals("organization") => subFields += ("authorAffiliation" -> PrimitiveFieldSingleValue("authorAffiliation", false, "primitive", (node \ "name").head.text))
          case e @ Elem("dcx-dai", "DAI", _, _, _) =>
            subFields += ("authorIdentifierScheme" -> PrimitiveFieldSingleValue("authorIdentifierScheme", multiple = false, "controlledVocabulary", e.label))
            subFields += ("authorIdentifier" -> PrimitiveFieldSingleValue("authorIdentifier", multiple = false, "primitive", e.text))
          case e @ Elem("dcx-dai", "ISNI", _, _, _) =>
            subFields += ("authorIdentifierScheme" -> PrimitiveFieldSingleValue("authorIdentifierScheme", multiple = false, "controlledVocabulary", e.label))
            subFields += ("authorIdentifier" -> PrimitiveFieldSingleValue("authorIdentifier", multiple = false, "primitive", e.text))
          case e @ Elem("dcx-dai", "ORCID", _, _, _) =>
            subFields += ("authorIdentifierScheme" -> PrimitiveFieldSingleValue("authorIdentifierScheme", multiple = false, "controlledVocabulary", e.label))
            subFields += ("authorIdentifier" -> PrimitiveFieldSingleValue("authorIdentifier", multiple = false, "primitive", e.text))
          case _ => ()
        }
        objectList += subFields.toMap
      })
      citationFields += CompoundField("author", multiple = true, "compound", objectList.toList)
    }
  }

  def addContributors(node: Node): Try[Unit] = Try {
    val contributorDetails = node \\ "contributorDetails"
    val objectList = new ListBuffer[Map[String, Field]]()
    if (contributorDetails.nonEmpty) {
      contributorDetails.map(contributorNode => {
        var subFields = collection.mutable.Map[String, Field]()
        val hasOrganization = (contributorNode \\ "name").exists(_.text.nonEmpty)
        val hasRole = (contributorNode \\ "role").exists(_.text.nonEmpty)

        //is surname verplicht?
        val hasContributor = (contributorNode \\ "surname").exists(_.text.nonEmpty)
        if (hasContributor) {
          subFields += ("contributorName" -> PrimitiveFieldSingleValue("contributorName", multiple = false, "primitive", getAuthorName((contributorNode \ "author").head)))
        }
        if (hasOrganization && !hasContributor) {
          subFields += ("contributorName" -> PrimitiveFieldSingleValue("contributorName", multiple = false, "primitive", (node \\ "name").head.text))
        }
        if (hasRole) {
          subFields += ("contributorType" -> PrimitiveFieldSingleValue("contributorType", multiple = false, "controlledVocabulary", (node \\ "role").head.text))
        }
        objectList += subFields.toMap
      })
      citationFields += CompoundField("contributor", multiple = true, "compound", objectList.toList)
    }
  }

  def addAlternativeIdentifier(node: Node): Try[Unit] = Try {
    val objectList = new ListBuffer[Map[String, Field]]()
    val alternativeIdentifiers = node \\ "isFormatOf"
    if (alternativeIdentifiers.nonEmpty) {
      alternativeIdentifiers.foreach(contributorNode => {
        val idValue = contributorNode.head.text
        if (idValue.nonEmpty) {
          var subFields = collection.mutable.Map[String, Field]()
          subFields += ("otherIdAgency" -> PrimitiveFieldSingleValue("otherIdAgency", multiple = false, "primitive", "DAI"))
          subFields += ("otherIdValue" -> PrimitiveFieldSingleValue("otherIdValue", multiple = false, "primitive", idValue))
          objectList += subFields.toMap
        }
      })
      citationFields += CompoundField("otherId", multiple = true, "compound", objectList.toList)
    }
  }

  //todo create mapping from date elements in EASY to date elements in DV
  def addDates(node: Node): Try[Unit] = Try {
    val objectList = new ListBuffer[Map[String, Field]]()
    val dateElements = (node \\ "_").collect {
      case e @ Elem("dcterms", "dateAccepted", _, _, _) if e.attributes.nonEmpty => ("Date accepted", e.text)
      case e @ Elem("dcterms", "valid", _, _, _) if e.attributes.nonEmpty => ("Valid", e.text)
      case e @ Elem("dcterms", "issued", _, _, _) if e.attributes.nonEmpty => ("Issued", e.text)
      case e @ Elem("dcterms", "modified", _, _, _) if e.attributes.nonEmpty => ("Modified", e.text)
      case e @ Elem("dc", "date", _, _, _) if e.attributes.nonEmpty => ("Date", e.text)
    }
    if (dateElements.nonEmpty) {
      dateElements.foreach(date => {
        var subFields = collection.mutable.Map[String, Field]()
        subFields += ("easy-date-event" -> PrimitiveFieldSingleValue("easy-date-event", multiple = false, "controlledVocabulary", date._1))
        subFields += ("esy-date-val" -> PrimitiveFieldSingleValue("esy-date-val", multiple = false, "primitive", date._2))
        objectList += subFields.toMap
      })
      basicInformationFields += CompoundField("easy-date", multiple = true, "compound", objectList.toList)
    }
  }

  def addDatesFreeFormat(node: Node): Try[Unit] = Try {
    val objectList = new ListBuffer[Map[String, Field]]()
    val dateElements = (node \\ "_").collect {
      case e @ Elem("dcterms", "dateAccepted", _, _, _) if e.attributes.isEmpty => ("Date accepted", e.text)
      case e @ Elem("dcterms", "valid", _, _, _) if e.attributes.isEmpty => ("Valid", e.text)
      case e @ Elem("dcterms", "issued", _, _, _) if e.attributes.isEmpty => ("Issued", e.text)
      case e @ Elem("dcterms", "modified", _, _, _) if e.attributes.isEmpty => ("Modified", e.text)
      case e @ Elem("dc", "date", _, _, _) if e.attributes.isEmpty => ("Date", e.text)
    }
    if (dateElements.nonEmpty) {
      dateElements.foreach(date => {
        var subFields = collection.mutable.Map[String, Field]()
        subFields += ("easy-date-event-free" -> PrimitiveFieldSingleValue("easy-date-event-free", multiple = false, "controlledVocabulary", date._1))
        subFields += ("easy-date-val-free" -> PrimitiveFieldSingleValue("easy-date-val-free", multiple = false, "primitive", date._2))
        objectList += subFields.toMap
      })
      basicInformationFields += CompoundField("easy-date-free", multiple = true, "compound", objectList.toList)
    }
  }

  def addRelatedIdentifiers(node: Node): Try[Unit] = Try {
    val relIdList = new ListBuffer[Map[String, Field]]()
    val relIdUrlList = new ListBuffer[Map[String, Field]]()

    val relationElements = (node \\ "_").collect {
      case e @ Elem(_, "conformsTo", _, _, _) if isRelatedIdentifier(e.attributes) => RelatedIdentifier("relation", mapScheme(e.attributes).toString, e.text, isRelatedIdentifier = true)
      case e @ Elem(_, "relation", _, _, _) if isRelatedIdentifier(e.attributes) => RelatedIdentifier("relation", mapScheme(e.attributes).toString, e.text, isRelatedIdentifier = false)
      case e @ Elem(_, "relation", _, _, _) if !isRelatedIdentifier(e.attributes) => RelatedIdentifier("relation", getUrl(e.attributes), e.text, isRelatedIdentifier = false)
    }
    if (relationElements.nonEmpty) {
      relationElements.foreach(relation => {
        var subFields = collection.mutable.Map[String, Field]()
        if (relation.isRelatedIdentifier) {
          subFields += ("easy-relid-relation" -> PrimitiveFieldSingleValue("easy-relid-relation", multiple = false, "controlledVocabulary", relation.relationType))
          subFields += ("easy-relid-type" -> PrimitiveFieldSingleValue("easy-relid-type", multiple = false, "controlledVocabulary", relation.schemeOrUrl))
          subFields += ("easy-relid-relatedid" -> PrimitiveFieldSingleValue("easy-relid-relatedid", multiple = false, "primitive", relation.value))
          relIdList += subFields.toMap
        }
        else {
          subFields += ("easy-relid-relation-url" -> PrimitiveFieldSingleValue("easy-relid-relation-url", multiple = false, "controlledVocabulary", relation.relationType))
          subFields += ("easy-relid-url-title" -> PrimitiveFieldSingleValue("easy-relid-url-title", multiple = false, "primitive", relation.value))
          subFields += ("easy-relid-url-url" -> PrimitiveFieldSingleValue("easy-relid-url-url", multiple = false, "primitive", relation.schemeOrUrl))
          relIdUrlList += subFields.toMap
        }
      })
      basicInformationFields += CompoundField("easy-relid", multiple = true, "compound", relIdList.toList)
      basicInformationFields += CompoundField("easy-relid-url", multiple = true, "compound", relIdUrlList.toList)
    }
  }

  def getAuthorName(author: Node): String = {
    var authorName = new ListBuffer[String]
    author.nonEmptyChildren.foreach {
      case e @ Elem(_, "titles", _, _, _) => authorName += e.text
      case e @ Elem(_, "initials", _, _, _) => authorName += e.text
      case e @ Elem(_, "surname", _, _, _) => authorName += e.text
      case _ => ()
    }
    authorName.mkString(", ")
  }

  //check if RelatedIdentifier or Relation
  def isRelatedIdentifier(md: MetaData): Boolean = {
    md.get("xsi-type").nonEmpty || md.get("scheme").nonEmpty
  }

  def mapScheme(md: MetaData): IdScheme.Value = {
    val attr = md.asAttrMap.filter(a => a._1 == "scheme" | (a._1 == "xsi:type"))
    attr.head._2 match {
      case "id-type:NWO-PROJECTNR" => IdScheme.NWO
      case "id-type:ISBN" => IdScheme.ISBN
      case "id-type:ISSN" => IdScheme.ISSN
      case "DOI" | "id-type:DOI" => IdScheme.DOI
      case "URN" => IdScheme.URN
      case "id-type:other" => IdScheme.OTHER
      case _ => IdScheme.DEFAULT
    }
  }

  def getUrl(md: MetaData): String = {
    md.get("href").getOrElse("").asInstanceOf[String]
  }

  //assigns correct values to Coordinate (RD = [x,y) and Degrees = [y,x]
  def getPointCoordinate(isDegree: Boolean, values: List[String]): PointCoordinate = {
    if (isDegree) {
      PointCoordinate(values.head, values(1))
    }
    else {
      PointCoordinate(values(1), values.head)
    }
  }

  def getBoxCoordinate(isRD: Boolean, lower: List[String], upper: List[String]): BoxCoordinate = {
    if (isRD) {
      BoxCoordinate(upper(1), lower(1), upper.head, lower.head)
    }
    else {
      BoxCoordinate(upper.head, lower.head, upper(1), lower(1))
    }
  }
}
