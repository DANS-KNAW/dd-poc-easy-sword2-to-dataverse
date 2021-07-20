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

import nl.knaw.dans.easy.dd2d.fieldbuilders.{ AbstractFieldBuilder, CompoundFieldBuilder, CvFieldBuilder, PrimitiveFieldBuilder }
import nl.knaw.dans.easy.dd2d.mapping._
import nl.knaw.dans.lib.dataverse.model.dataset.{ Dataset, DatasetVersion, MetadataBlock }
import org.apache.commons.lang.StringUtils

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.language.postfixOps
import scala.util.Try
import scala.xml.{ Elem, Node, NodeSeq }

/**
 * Creates dataset level metadata for Dataverse from information in the deposit.
 *
 * @param activeMetadataBlocks    the metadata blocks that are active in the target dataverse
 * @param narcisClassification    NARCIS classification SKOS, currently not used
 * @param iso2ToDataverseLanguage map from ISO639-2 to the Dataverse language terms
 * @param reportIdToTerm          map from Cultureel Erfgoed Report Type ID to the human readable term
 */
class DepositToDvDatasetMetadataMapper(activeMetadataBlocks: List[String],
                                       narcisClassification: Elem,
                                       iso1ToDataverseLanguage: Map[String, String],
                                       iso2ToDataverseLanguage: Map[String, String],
                                       reportIdToTerm: Map[String, String]) extends BlockCitation // TODO: not necessary anymore?
  with BlockArchaeologySpecific
  with BlockTemporalAndSpatial
  with BlockRights
  with BlockRelation
  with BlockDataVaultMetadata {
  lazy val citationFields = new mutable.HashMap[String, AbstractFieldBuilder]()
  lazy val rightsFields = new mutable.HashMap[String, AbstractFieldBuilder]()
  lazy val relationFields = new mutable.HashMap[String, AbstractFieldBuilder]()
  lazy val archaeologySpecificFields = new mutable.HashMap[String, AbstractFieldBuilder]()
  lazy val temporalSpatialFields = new mutable.HashMap[String, AbstractFieldBuilder]()
  lazy val dataVaultFields = new mutable.HashMap[String, AbstractFieldBuilder]()

  def toDataverseDataset(ddm: Node, optAgreements: Option[Node], optDateOfDeposit: Option[String], contactData: List[JsonObject], vaultMetadata: VaultMetadata): Try[Dataset] = Try {
    // Please, keep ordered by order in Dataverse UI as much as possible!

    if (activeMetadataBlocks.contains("citation")) {
      val titles = ddm \ "profile" \ "title"
      checkRequiredField(TITLE, titles)

      val alternativeTitles = (ddm \ "dcmiMetadata" \ "title") ++ (ddm \ "dcmiMetadata" \ "alternative")

      addPrimitiveFieldSingleValue(citationFields, TITLE, titles.head)
      addPrimitiveFieldSingleValue(citationFields, ALTERNATIVE_TITLE, alternativeTitles)
      addCompoundFieldMultipleValues(citationFields, OTHER_ID, DepositPropertiesVaultMetadata.toOtherIdValue(vaultMetadata.dataverseOtherId).toList)
      addCompoundFieldMultipleValues(citationFields, OTHER_ID, (ddm \ "dcmiMetadata" \ "identifier").filter(Identifier canBeMappedToOtherId), Identifier toOtherIdValue)
      addCompoundFieldMultipleValues(citationFields, AUTHOR, ddm \ "profile" \ "creatorDetails" \ "author", DcxDaiAuthor toAuthorValueObject)
      addCompoundFieldMultipleValues(citationFields, AUTHOR, ddm \ "profile" \ "creatorDetails" \ "organization", DcxDaiOrganization toAuthorValueObject)
      addCompoundFieldMultipleValues(citationFields, AUTHOR, ddm \ "profile" \ "creator", Creator toAuthorValueObject)
      addCompoundFieldMultipleValues(citationFields, DATASET_CONTACT, contactData)
      addCompoundFieldMultipleValues(citationFields, DESCRIPTION, ddm \ "profile" \ "description", Description toDescriptionValueObject)
      addCompoundFieldMultipleValues(citationFields, DESCRIPTION, if (alternativeTitles.isEmpty) NodeSeq.Empty
                                                                  else alternativeTitles.tail, Description toDescriptionValueObject)
      val otherDescriptions = (ddm \ "dcmiMetadata" \ "date") ++
        (ddm \ "dcmiMetadata" \ "dateAccepted") ++
        (ddm \ "dcmiMetadata" \ "dateCopyrighted ") ++
        (ddm \ "dcmiMetadata" \ "dateSubmitted") ++
        (ddm \ "dcmiMetadata" \ "modified") ++
        (ddm \ "dcmiMetadata" \ "issued") ++
        (ddm \ "dcmiMetadata" \ "valid") ++
        (ddm \ "dcmiMetadata" \ "coverage")
      addCompoundFieldMultipleValues(citationFields, DESCRIPTION, otherDescriptions, Description toPrefixedDescription)

      checkRequiredField(SUBJECT, ddm \ "profile" \ "audience")
      addCvFieldMultipleValues(citationFields, SUBJECT, ddm \ "profile" \ "audience", Audience toCitationBlockSubject)
      addCompoundFieldMultipleValues(citationFields, KEYWORD, (ddm \ "dcmiMetadata" \ "subject").filter(Subject hasNoCvAttributes), Subject toKeyWordValue)
      addCompoundFieldMultipleValues(citationFields, KEYWORD, (ddm \ "dcmiMetadata" \ "language").filterNot(Language isIsoLanguage), Language toKeywordValue)
      addCvFieldMultipleValues(citationFields, LANGUAGE, ddm \ "dcmiMetadata" \ "language", Language.toCitationBlockLanguage(iso1ToDataverseLanguage, iso2ToDataverseLanguage))
      addPrimitiveFieldSingleValue(citationFields, PRODUCTION_DATE, ddm \ "profile" \ "created", DateTypeElement toYearMonthDayFormat)
      addCompoundFieldMultipleValues(citationFields, CONTRIBUTOR, (ddm \ "dcmiMetadata" \ "contributorDetails" \ "author").filterNot(DcxDaiAuthor isRightsHolder), DcxDaiAuthor toContributorValueObject)
      addCompoundFieldMultipleValues(citationFields, CONTRIBUTOR, (ddm \ "dcmiMetadata" \ "contributorDetails" \ "organization").filterNot(DcxDaiOrganization isRightsHolder), DcxDaiOrganization toContributorValueObject)
      addPrimitiveFieldSingleValue(citationFields, DISTRIBUTION_DATE, ddm \ "profile" \ "available", DateTypeElement toYearMonthDayFormat)

      addPrimitiveFieldSingleValue(citationFields, DATE_OF_DEPOSIT, optDateOfDeposit)
      // TODO: what to set dateOfDeposit to for SWORD or multi-deposits? Take from deposit.properties?

      addPrimitiveFieldMultipleValues(citationFields, DATA_SOURCES, ddm \ "dcmiMetadata" \ "source")

      addCompoundFieldMultipleValues(citationFields, PUBLICATION, (ddm \ "dcmiMetadata" \ "identifier").filter(Identifier isRelatedPublication), Identifier toRelatedPublicationValue)
      addCompoundFieldMultipleValues(citationFields, GRANT_NUMBER, (ddm \ "dcmiMetadata" \ "identifier").filter(Identifier isNwoGrantNumber), Identifier toNwoGrantNumberValue)
    }
    else {
      throw new IllegalStateException("Metadatablock citation should always be active")
    }

    if (activeMetadataBlocks.contains("dansRights")) {
      checkRequiredField(RIGHTS_HOLDER, ddm \ "dcmiMetadata" \ "rightsHolder")
      addPrimitiveFieldMultipleValues(rightsFields, RIGHTS_HOLDER, ddm \ "dcmiMetadata" \ "rightsHolder", AnyElement toText)
      optAgreements.foreach { agreements =>
        addCvFieldSingleValue(rightsFields, PERSONAL_DATA_PRESENT, agreements \ "personalDataStatement", PersonalStatement toHasPersonalDataValue)
      }
      addPrimitiveFieldMultipleValues(rightsFields, RIGHTS_HOLDER, (ddm \ "dcmiMetadata" \ "contributorDetails" \ "author").filter(DcxDaiAuthor isRightsHolder), DcxDaiAuthor toRightsHolder)
      addPrimitiveFieldMultipleValues(rightsFields, RIGHTS_HOLDER, (ddm \ "dcmiMetadata" \ "contributorDetails" \ "organization").filter(DcxDaiOrganization isRightsHolder), DcxDaiOrganization toRightsHolder)
      addCvFieldMultipleValues(rightsFields, LANGUAGE_OF_METADATA, (ddm \ "profile" \ "_") ++ (ddm \ "dcmiMetadata" \ "_"), Language.langAttributeToMetadataLanguage(iso1ToDataverseLanguage, iso2ToDataverseLanguage))
    }

    if (activeMetadataBlocks.contains("dansRelationMetadata")) {
      addPrimitiveFieldMultipleValues(relationFields, AUDIENCE , ddm \ "profile" \ "audience", Audience toNarcisTerm)
      addPrimitiveFieldMultipleValues(relationFields, COLLECTION, ddm \ "dcmiMetadata" \ "inCollection", InCollection toCollection)
      addCompoundFieldMultipleValues(relationFields, RELATION, (ddm \ "dcmiMetadata" \ "_").filter(Relation isRelation), Relation toRelationValueObject)
    }

    if (activeMetadataBlocks.contains("dansArchaeologyMetadata")) {
      addPrimitiveFieldMultipleValues(archaeologySpecificFields, ARCHIS_ZAAK_ID, (ddm \ "dcmiMetadata" \ "identifier").filter(Identifier isArchisZaakId), Identifier toArchisZaakId)
      addCompoundFieldMultipleValues(archaeologySpecificFields, ARCHIS_NUMBER, (ddm \ "dcmiMetadata" \ "identifier").filter(Identifier isArchisNumber), Identifier toArchisNumberValue)
      addPrimitiveFieldMultipleValues(archaeologySpecificFields, ABR_RAPPORT_TYPE, (ddm \ "dcmiMetadata" \ "reportNumber").filter(AbrReportType isAbrReportType), AbrReportType toAbrRapportType)
      addPrimitiveFieldMultipleValues(archaeologySpecificFields, ABR_RAPPORT_NUMMER, ddm \ "dcmiMetadata" \ "reportNumber")
      addPrimitiveFieldMultipleValues(archaeologySpecificFields, ABR_VERWERVINGSWIJZE, (ddm \ "dcmiMetadata" \ "acquisitionMethod").filter(AbrAcquisitionMethod isAbrVerwervingswijze), AbrAcquisitionMethod toVerwervingswijze)
      addPrimitiveFieldMultipleValues(archaeologySpecificFields, ABR_COMPLEX, (ddm \ "dcmiMetadata" \ "subject").filter(SubjectAbr isAbrComplex), SubjectAbr toAbrComplex)
      addPrimitiveFieldMultipleValues(archaeologySpecificFields, ABR_PERIOD, (ddm \ "dcmiMetadata" \ "temporal").filter(TemporalAbr isAbrPeriod), TemporalAbr toAbrPeriod)
    }

    if (activeMetadataBlocks.contains("dansTemporalSpatial")) {
      addPrimitiveFieldMultipleValues(temporalSpatialFields, TEMPORAL_COVERAGE, ddm \ "dcmiMetadata" \ "temporal")
      addCompoundFieldMultipleValues(temporalSpatialFields, SPATIAL_POINT, (ddm \ "dcmiMetadata" \ "spatial").filter(_.child.exists(_.label == "Point")), SpatialPoint toEasyTsmSpatialPointValueObject)
      addCompoundFieldMultipleValues(temporalSpatialFields, SPATIAL_BOX, ddm \ "dcmiMetadata" \ "spatial" \ "boundedBy", SpatialBox toEasyTsmSpatialBoxValueObject)
      addCvFieldMultipleValues(temporalSpatialFields, SPATIAL_COVERAGE_CONTROLLED, (ddm \ "dcmiMetadata" \ "spatial").filterNot(_.child.exists(_.isInstanceOf[Elem])), SpatialCoverage toControlledSpatialValue)
      addPrimitiveFieldMultipleValues(temporalSpatialFields, SPATIAL_COVERAGE_UNCONTROLLED, (ddm \ "dcmiMetadata" \ "spatial").filterNot(_.child.exists(_.isInstanceOf[Elem])), SpatialCoverage toUncontrolledSpatialValue)
    }

    if (activeMetadataBlocks.contains("dansDataVaultMetadata")) {
      addPrimitiveFieldSingleValue(dataVaultFields, BAG_ID, Option(vaultMetadata.dataverseBagId))
      addPrimitiveFieldSingleValue(dataVaultFields, NBN, Option(vaultMetadata.dataverseNbn))
      addPrimitiveFieldSingleValue(dataVaultFields, DANS_OTHER_ID, Option(vaultMetadata.dataverseOtherId))
      addPrimitiveFieldSingleValue(dataVaultFields, DANS_OTHER_ID_VERSION, Option(vaultMetadata.dataverseOtherIdVersion))
      addPrimitiveFieldSingleValue(dataVaultFields, SWORD_TOKEN, Option(vaultMetadata.dataverseSwordToken))
    }
    else {
      throw new IllegalStateException("Metadatablock dansDataVaultMetadata should always be active")
    }

    assembleDataverseDataset()
  }

  private def checkRequiredField(fieldName: String, nodes: NodeSeq): Unit = {
    if (nodes.isEmpty || nodes.map(_.text).forall(StringUtils.isBlank)) throw MissingRequiredFieldException(fieldName)
  }

  private def assembleDataverseDataset(): Dataset = {
    val versionMap = mutable.Map[String, MetadataBlock]()
    addMetadataBlock(versionMap, "citation", "Citation Metadata", citationFields)
    addMetadataBlock(versionMap, "dansRights", "Rights Metadata", rightsFields)
    addMetadataBlock(versionMap, "dansRelationMetadata", "Relation Metadata", relationFields)
    addMetadataBlock(versionMap, "dansArchaeologyMetadata", "Archaeology-Specific Metadata", archaeologySpecificFields)
    addMetadataBlock(versionMap, "dansTemporalSpatial", "Temporal and Spatial Coverage", temporalSpatialFields)
    addMetadataBlock(versionMap, "dansDataVaultMetadata", "Data Vault Metadata", dataVaultFields)
    val datasetVersion = DatasetVersion(metadataBlocks = versionMap.toMap)
    Dataset(datasetVersion)
  }

  private def addPrimitiveFieldSingleValue(metadataBlockFields: mutable.HashMap[String, AbstractFieldBuilder], name: String, sourceNodes: NodeSeq, nodeTransformer: Node => Option[String] = AnyElement toText): Unit = {
    sourceNodes
      .map(nodeTransformer)
      .filter(_.isDefined)
      .map(_.get)
      .filterNot(StringUtils.isBlank)
      .take(1)
      .foreach(v => {
        metadataBlockFields.getOrElseUpdate(name, new PrimitiveFieldBuilder(name, multipleValues = false)) match {
          case b: PrimitiveFieldBuilder => b.addValue(v)
          case _ => throw new IllegalArgumentException("Trying to add non-primitive value(s) to primitive field")
        }
      })
  }

  private def addPrimitiveFieldSingleValue(metadataBlockFields: mutable.HashMap[String, AbstractFieldBuilder], name: String, value: Option[String]): Unit = {
    value.filterNot(StringUtils.isBlank).foreach { v =>
      metadataBlockFields.getOrElseUpdate(name, new PrimitiveFieldBuilder(name, multipleValues = false)) match {
        case b: PrimitiveFieldBuilder => b.addValue(v)
        case _ => throw new IllegalArgumentException("Trying to add non-primitive value(s) to primitive field")
      }
    }
  }

  private def addPrimitiveFieldMultipleValues(metadataBlockFields: mutable.HashMap[String, AbstractFieldBuilder], name: String, sourceNodes: NodeSeq, nodeTransformer: Node => Option[String] = AnyElement toText): Unit = {
    val values = sourceNodes.map(nodeTransformer).filter(_.isDefined).map(_.get).toList
    values.filterNot(StringUtils.isBlank).foreach { v =>
      metadataBlockFields.getOrElseUpdate(name, new PrimitiveFieldBuilder(name, multipleValues = true)) match {
        case b: PrimitiveFieldBuilder => b.addValue(v)
        case _ => throw new IllegalArgumentException("Trying to add non-primitive value(s) to primitive field")
      }
    }
  }

  private def addCvFieldSingleValue(metadataBlockFields: mutable.HashMap[String, AbstractFieldBuilder], name: String, sourceNodes: NodeSeq, nodeTransformer: Node => Option[String]): Unit = {
    val values = sourceNodes.map(nodeTransformer).filter(_.isDefined).map(_.get).toList
    metadataBlockFields.getOrElseUpdate(name, new CvFieldBuilder(name, multipleValues = false)) match {
      case cfb: CvFieldBuilder => values.filterNot(StringUtils.isBlank).foreach(cfb.addValue)
      case _ => throw new IllegalArgumentException("Trying to add non-controlled-vocabulary value(s) to controlled vocabulary field")
    }
  }

  private def addCvFieldMultipleValues(metadataBlockFields: mutable.HashMap[String, AbstractFieldBuilder], name: String, sourceNodes: NodeSeq, nodeTransformer: Node => Option[String]): Unit = {
    val values = sourceNodes.map(nodeTransformer).filter(_.isDefined).map(_.get).toList
    metadataBlockFields.getOrElseUpdate(name, new CvFieldBuilder(name)) match {
      case cfb: CvFieldBuilder => values.filterNot(StringUtils.isBlank).foreach(cfb.addValue)
      case _ => throw new IllegalArgumentException("Trying to add non-controlled-vocabulary value(s) to controlled vocabulary field")
    }
  }

  private def addCompoundFieldMultipleValues(fields: mutable.HashMap[String, AbstractFieldBuilder], name: String, sourceNodes: NodeSeq, nodeTransformer: Node => JsonObject): Unit = {
    val valueObjects = new ListBuffer[JsonObject]()
    sourceNodes.foreach(e => valueObjects += nodeTransformer(e))
    fields.getOrElseUpdate(name, new CompoundFieldBuilder(name)) match {
      case cfb: CompoundFieldBuilder => valueObjects.foreach(cfb.addValue)
      case _ => throw new IllegalArgumentException("Trying to add non-compound value(s) to compound field")
    }
  }

  private def addCompoundFieldMultipleValues(fields: mutable.HashMap[String, AbstractFieldBuilder], name: String, valueObjects: List[JsonObject]): Unit = {
    fields.getOrElseUpdate(name, new CompoundFieldBuilder(name)) match {
      case cfb: CompoundFieldBuilder => valueObjects.foreach(cfb.addValue)
      case _ => throw new IllegalArgumentException("Trying to add non-compound value(s) to compound field")
    }
  }

  private def addMetadataBlock(versionMap: mutable.Map[String, MetadataBlock], blockId: String, blockDisplayName: String, fields: mutable.HashMap[String, AbstractFieldBuilder]): Unit = {
    if (fields.nonEmpty) {
      versionMap.put(blockId, MetadataBlock(blockDisplayName, fields.values.map(_.build()).filter(_.isDefined).map(_.get).toList))
    }
  }
}
