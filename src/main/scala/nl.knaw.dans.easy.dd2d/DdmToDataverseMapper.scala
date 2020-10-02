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
import nl.knaw.dans.easy.dd2d.mapping._
import org.json4s.DefaultFormats

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Try
import scala.xml.{ Elem, MetaData, Node, NodeSeq }

/**
 * Maps DANS Dataset Metadata to Dataverse Json.
 */
// TODO: Rename if we also need to take elements from EMD
class DdmToDataverseMapper() {
  private implicit val format: DefaultFormats.type = DefaultFormats
  case class RelatedIdentifier(relationType: String, schemeOrUrl: String, value: String, isRelatedIdentifier: Boolean)
  case class PointCoordinate(x: String, y: String)
  case class BoxCoordinate(north: String, south: String, east: String, west: String)
  lazy val citationFields = new ListBuffer[Field]
  lazy val basicInformationFields = new ListBuffer[Field]
  lazy val archaeologySpecificFields = new ListBuffer[Field]
  lazy val temporalSpatialFields = new ListBuffer[Field]

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
    // Please keep ordered by order in Dataverse UI as much as possible

    // Citation
    addPrimitiveFieldSingleValue(citationFields, "title", ddm \ "profile" \ "title") // TODO: Conflict: is multi-value in DDM
    addCompoundFieldMultipleValues(citationFields, "dsDescription", DescriptionNodes(ddm \ "profile" \ "description").toValueObjects)
    addCompoundFieldMultipleValues(citationFields, "author", CreatorDetailsNodes(ddm \ "profile" \ "creatorDetails").toAuthorValueObjects)
    // TODO: creator unstructured
    addPrimitiveFieldSingleValue(citationFields, "productionDate", CreatedNodes(ddm \ "profile" \ "created").toProductionDates.headOption) // TODO: what if there are more than one?
    addPrimitiveFieldSingleValue(citationFields, "distributionDate", AvailableNodes(ddm \ "profile" \ "available").toDistributionDates.headOption) // TODO: correct target for available?
    addCvFieldMultipleValues(citationFields, "subject", AudienceNodes(ddm \ "profile" \ "audience").toCitationBlockSubjects)
    addPrimitiveFieldSingleValue(citationFields, "alternativeTitle", ddm \ "dcmiMetadata" \ "alternative")
    addPrimitiveFieldMultipleValues(citationFields, "dataSources", ddm \ "dcmiMetadata" \ "source")

    // Basic information
    addPrimitiveFieldMultipleValues(basicInformationFields, "languageOfFiles", LanguageNodes(ddm \ "dcmiMetadata" \ "language").toDataverseLanguages)

    // Archaeology specific
    addPrimitiveFieldMultipleValues(archaeologySpecificFields, "archisZaakId", IdentifierNodes(ddm \ "dcmiMetadata" \ "identifier").toArchisZaakIds)

    assembleDataverseDataset()
  }

  private def assembleDataverseDataset(): DataverseDataset = {
    val versionMap = mutable.Map[String, MetadataBlock]()
    addMetadataBlock(versionMap, "citation", "Citation Metadata", citationFields)
    addMetadataBlock(versionMap, "basicInformation", "Basic Information", basicInformationFields)
    addMetadataBlock(versionMap, "archaeologyMetadata", "Archaeology-Specific Metadata", archaeologySpecificFields)
    addMetadataBlock(versionMap, "temporal-spatial", "Temporal and Spatial Coverage", temporalSpatialFields)

    val datasetVersion = DatasetVersion(versionMap.toMap)
    DataverseDataset(datasetVersion)
  }

  private def addPrimitiveFieldSingleValue(metadataBlockFields: ListBuffer[Field], name: String, elements: NodeSeq): Unit = {
    if (elements.nonEmpty) {
      metadataBlockFields += PrimitiveFieldSingleValue(name, multiple = false, "primitive", elements.map(_.text).head)
    }
  }

  private def addPrimitiveFieldSingleValue(metadataBlockFields: ListBuffer[Field], name: String, valueOpt: Option[String]): Unit = {
    valueOpt.foreach(v => metadataBlockFields += PrimitiveFieldSingleValue(name, multiple = false, "primitive", v))
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

  def mapToCompoundFields(node: Node): Try[Unit] = Try {
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
