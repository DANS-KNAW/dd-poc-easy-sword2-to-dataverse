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

import nl.knaw.dans.easy.s2d.dataverse.json.{ CompoundField, DatasetVersion, DataverseDataset, Field, MetadataBlock, PrimitiveFieldMultipleValues, PrimitiveFieldSingleValue }
import org.json4s.DefaultFormats
import org.json4s.native.Serialization

import scala.collection.mutable.ListBuffer
import scala.util.Try
import scala.xml.{ Elem, MetaData, Node }

/**
 * Maps Easy xml to Dataverse Json
 */
class EasyToDataverseMapper() {

  implicit val format = DefaultFormats
  case class RelatedIdentifier(relationType: String, schemeOrUrl: String, value: String, isRelatedIdentifier: Boolean)
  case class PointCoordinate(x: String, y: String)
  case class BoxCoordinate(north: String, south: String, east: String, west: String)
  lazy val citationFields = new ListBuffer[Field]
  lazy val access_and_LicenceFields = new ListBuffer[Field]
  lazy val depositAgreementFields = new ListBuffer[Field]
  lazy val basicInformationFields = new ListBuffer[Field]
  lazy val archaeologySpecificMetadata = new ListBuffer[Field]
  lazy val temporalSpatialFields = new ListBuffer[Field]

  object Spatial {
    /** coordinate order y, x = latitude (DCX_SPATIAL_Y), longitude (DCX_SPATIAL_X) */
    val DEGREES_SRS_NAME = "http://www.opengis.net/def/crs/EPSG/0/4326"

    /** coordinate order x, y = longitude (DCX_SPATIAL_X), latitude (DCX_SPATIAL_Y) */
    val RD_SRS_NAME = "http://www.opengis.net/def/crs/EPSG/0/28992"
  }

  /**
   * Converts easy-ddm xml to Scala case classes which at the end
   * are converted to json using the Json4s library
   *
   * @param node easy-ddm
   * @return Json String
   */
  def mapToJson(node: Node): Try[String] = Try {
    // Todo Which title: DV allows only one
    val title = (node \\ "title").head.text
    addPrimitiveFieldToMetadataBlock("title", multi = false, "primitive", Some(title), None, "citation")

    mapToPrimitiveFieldsSingleValue(node)
    mapToPrimitiveFieldsMultipleValues(node)
    mapToCompoundFields(node)

    val citationBlock = MetadataBlock("Citation Metadata", citationFields.toList)
    val access_and_licenseBlock = MetadataBlock("Access and License", access_and_LicenceFields.toList)
    val depositAgreementBlock = MetadataBlock("Deposit Agreement", depositAgreementFields.toList)
    val basicInformation = MetadataBlock("Basic Information", basicInformationFields.toList)
    val archaeologyMetadataBlock = MetadataBlock("archaeologyMetadata", archaeologySpecificMetadata.toList)
    val temporalSpatialBlock = MetadataBlock("Temporal and Spatial Coverage", temporalSpatialFields.toList)
    val datasetVersion = DatasetVersion(Map("citation" -> citationBlock, "basicInformation" -> basicInformation, "depositAgreement" -> depositAgreementBlock, "access-and-license" -> access_and_licenseBlock, "archaeologyMetadata" -> archaeologyMetadataBlock, "temporal-spatial" -> temporalSpatialBlock))
    val dataverseDataset = DataverseDataset(datasetVersion)
    println(Serialization.writePretty(dataverseDataset))
    Serialization.writePretty(dataverseDataset)
  }

  def mapToPrimitiveFieldsSingleValue(node: Node): Try[Unit] = Try {
    (node \\ "_").foreach {
      case e @ Elem("dcterms", "alternative", _, _, _) => addPrimitiveFieldToMetadataBlock("alternativeTitle", multi = false, "primitive", Some(e.text), None, "citation")
      //case node @ _ if node.label.equals("creatorDetails") => addCreator(node)
      case e @ Elem("ddm", "created", _, _, _) => addPrimitiveFieldToMetadataBlock("productionDate", multi = false, "primitive", Some(e.text), None, "citation")
      case e @ Elem("ddm", "accessRights", _, _, _) => addPrimitiveFieldToMetadataBlock("accessrights", multi = false, "controlledVocabulary", Some(e.text), None, "access_and_licence")
      case e @ Elem("dcterms", "license", _, _, _) => addPrimitiveFieldToMetadataBlock("license", multi = false, "controlledVocabulary", Some(e.text), None, "access_and_licence")
      //Todo get the correct element
      case e @ Elem(_, "instructionalMethod", _, _, _) => addPrimitiveFieldToMetadataBlock("accept", multi = false, "controlledVocabulary", Some(e.text), None, "depositAgreement")
      case _ => ()
    }
    //just search in "profile" because "dcmiMetadata" can also contain data available
    (node \ "profile").head.nonEmptyChildren.foreach {
      case e @ Elem("ddm", "available", _, _, _) => addPrimitiveFieldToMetadataBlock("dateavailable", multi = false, "primitive", Some(e.text), None, "access_and_licence")
      case _ => ()
    }

    val languageOfDescription: String = (node \\ "description")
      .headOption
      .map(_.attributes.filter(_.key == "lang"))
      .map(_.value).getOrElse("")
      .toString

    addPrimitiveFieldToMetadataBlock("languageofmetadata", multi = false, "controlledVocabulary", Some(languageOfDescription), None, "basicInformation")
  }

  def mapToPrimitiveFieldsMultipleValues(node: Node): Try[Unit] = Try {
    // Todo create mapping for EASY Dropdown values (d2700 etc.) to DV values
    val audience = (node \\ "audience").filter(e => !e.text.equals("")).map(_.text).toList
    if (audience.nonEmpty)
      addPrimitiveFieldToMetadataBlock("subject", multi = true, "controlledVocabulary", None, Some(audience), "citation")

    val language = (node \\ "language").filter(e => !e.text.equals("")).map(_.text).toList
    if (language.nonEmpty)
      addPrimitiveFieldToMetadataBlock("language", multi = true, "controlledVocabulary", None, Some(language), "citation")

    val source = (node \\ "source").filter(e => !e.text.equals("")).map(_.text).toList
    if (source.nonEmpty)
      addPrimitiveFieldToMetadataBlock("dataSources", multi = true, "primitive", None, Some(source), "citation")

    val zaakId = (node \\ "_").filter(_.attributes.exists(_.value.text == "id-type:ARCHIS-ZAAK-IDENTIFICATIE")).filter(e => !e.text.equals("")).toList.map(_.text)
    if (zaakId.nonEmpty)
      addPrimitiveFieldToMetadataBlock("archisZaakId", multi = true, "primitive", None, Some(zaakId), "archaeologyMetadata")

    val abrComplex = (node \\ "_").filter(_.attributes.exists(_.value.text == "abr:ABRcomplex")).filter(e => !e.text.equals("")).toList.map(_.text)
    if (abrComplex.nonEmpty)
      addPrimitiveFieldToMetadataBlock("subjectAbr", multi = true, "controlledVocabulary", None, Some(abrComplex), "archaeologyMetadata")

    val period = (node \\ "temporal").filter(_.attributes.exists(_.value.text == "abr:ABRperiode")).filter(e => !e.text.equals("")).toList.map(_.text)
    if (period.nonEmpty)
      addPrimitiveFieldToMetadataBlock("period", multi = true, "controlledVocabulary", None, Some(period), "archaeologyMetadata")

    val temporalCoverage = (node \\ "temporal").filterNot(_.attributes.exists(_.value.text == "abr:ABRperiode")).filter(e => !e.text.equals("")).toList.map(_.text)
    if (temporalCoverage.nonEmpty)
      addPrimitiveFieldToMetadataBlock("temporal-coverage", true, "primitive", None, Some(temporalCoverage), "temporalSpatial")

    val languageOfFiles = (node \\ "language").filter(e => !e.text.equals("")).map(_.text).toList
    addPrimitiveFieldToMetadataBlock("languageFiles", multi = true, "primitive", None, Some(languageOfFiles), "basicInformation")
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

      val lowerCorner = (spatial \\ "lowerCorner").filter(!_.text.isEmpty).head.text.split(" +").take(2).toList
      val upperCorner = (spatial \\ "upperCorner").filter(!_.text.isEmpty).head.text.split(" +").take(2).toList
      val boxCoordinate = getBoxCoordinate(isRD, lowerCorner, upperCorner)
      subFields += ("easy-tsm-spatial-box-north" -> PrimitiveFieldSingleValue("easy-tsm-spatial-box-north", false, "primitive", boxCoordinate.north))
      subFields += ("easy-tsm-spatial-box-east" -> PrimitiveFieldSingleValue("easy-tsm-spatial-box-east", false, "primitive", boxCoordinate.east))
      subFields += ("easy-tsm-spatial-box-south" -> PrimitiveFieldSingleValue("easy-tsm-spatial-box-south", false, "primitive", boxCoordinate.south))
      subFields += ("easy-tsm-spatial-box-west" -> PrimitiveFieldSingleValue("easy-tsm-spatial-box-west", false, "primitive", boxCoordinate.west))
      objectList += subFields.toMap
    })
    addCompoundFieldToMetadataBlock("temporalSpatial", CompoundField("easy-spatial-box", multiple = true, "compound", objectList.toList))
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
    addCompoundFieldToMetadataBlock("temporalSpatial", CompoundField("easy-tsm", multiple = true, "compound", objectList.toList))
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
      addCompoundFieldToMetadataBlock("citation", CompoundField("keyword", multiple = true, "compound", objectList.toList))
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
      addCompoundFieldToMetadataBlock("basicInformation", CompoundField("easy-pno", multiple = true, "compound", objectList.toList))
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
      addCompoundFieldToMetadataBlock("citation", CompoundField("author", multiple = true, "compound", objectList.toList))
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
      addCompoundFieldToMetadataBlock("citation", CompoundField("contributor", multiple = true, "compound", objectList.toList))
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
      addCompoundFieldToMetadataBlock("citation", CompoundField("otherId", multiple = true, "compound", objectList.toList))
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
      addCompoundFieldToMetadataBlock("basicInformation", CompoundField("easy-date", multiple = true, "compound", objectList.toList))
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
      addCompoundFieldToMetadataBlock("basicInformation", CompoundField("easy-date-free", multiple = true, "compound", objectList.toList))
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
      addCompoundFieldToMetadataBlock("basicInformation", CompoundField("easy-relid", multiple = true, "compound", relIdList.toList))
      addCompoundFieldToMetadataBlock("basicInformation", CompoundField("easy-relid-url", multiple = true, "compound", relIdUrlList.toList))
    }
  }

  def addPrimitiveFieldToMetadataBlock(fieldName: String, multi: Boolean, typeClass: String, value: Option[String], values: Option[List[String]], metadataBlockName: String): Unit = {
    if (!multi)
      getMetadatablockFieldList(metadataBlockName) += PrimitiveFieldSingleValue(fieldName, multiple = multi, typeClass, value.getOrElse(""))
    else
      getMetadatablockFieldList(metadataBlockName) += PrimitiveFieldMultipleValues(fieldName, multiple = multi, typeClass, values.getOrElse(List()))
  }

  def addCompoundFieldToMetadataBlock(metadataBlockName: String, compoundField: CompoundField): Unit = {
    getMetadatablockFieldList(metadataBlockName) += compoundField
  }

  def getMetadatablockFieldList(name: String): ListBuffer[Field] = {
    name match {
      case "citation" => citationFields
      case "access_and_licence" => access_and_LicenceFields
      case "depositAgreement" => depositAgreementFields
      case "basicInformation" => basicInformationFields
      case "archaeologyMetadata" => archaeologySpecificMetadata
      case "temporalSpatial" => temporalSpatialFields
      case _ => new ListBuffer[Field]
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

  object Scheme extends Enumeration {
    type Scheme = Value
    val DOI = Value("doi")
    val URN = Value("urn:nbn:nl")
    val ISBN = Value("ISBN")
    val ISSN = Value("ISSN")
    val NWO = Value("NWO ProjectNr")
    val OTHER = Value("other")
    val DEFAULT = Value("")
  }

  //check if RelatedIdentifier or Relation
  def isRelatedIdentifier(md: MetaData): Boolean = {
    md.get("xsi-type").nonEmpty || md.get("scheme").nonEmpty
  }

  def mapScheme(md: MetaData): Scheme.Value = {
    val attr = md.asAttrMap.filter(a => a._1 == "scheme" | (a._1 == "xsi:type"))
    attr.head._2 match {
      case "id-type:NWO-PROJECTNR" => Scheme.NWO
      case "id-type:ISBN" => Scheme.ISBN
      case "id-type:ISSN" => Scheme.ISSN
      case "DOI" | "id-type:DOI" => Scheme.DOI
      case "URN" => Scheme.URN
      case "id-type:other" => Scheme.OTHER
      case _ => Scheme.DEFAULT
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

