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
import nl.knaw.dans.lib.dataverse.model.dataset.{ CompoundField, ControlledMultipleValueField, Dataset, DatasetVersion, MetadataBlock, MetadataField, PrimitiveMultipleValueField, PrimitiveSingleValueField }

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.language.postfixOps
import scala.util.Try
import scala.xml.{ Elem, Node, NodeSeq }

/**
 * Maps DANS Dataset Metadata to Dataverse JSON.
 */
// TODO: Rename if we also need to take elements from EMD
class DepositToDataverseMapper(narcisClassification: Elem, isoToDataverseLanguage: Map[String, String]) extends BlockCitation
  with BlockArchaeologySpecific
  with BlockTemporalAndSpatial
  with BlockRights
  with BlockDataVaultMetadata {
  lazy val citationFields = new ListBuffer[MetadataField]
  lazy val citationFields2 = new mutable.HashMap[String, AbstractFieldBuilder]()
  lazy val archaeologySpecificFields = new ListBuffer[MetadataField]
  lazy val archaeologySpecificFields2 = new mutable.HashMap[String, AbstractFieldBuilder]()
  lazy val temporalSpatialFields = new ListBuffer[MetadataField]
  lazy val temporalSpatialFields2 = new mutable.HashMap[String, AbstractFieldBuilder]()
  lazy val rightsFields = new ListBuffer[MetadataField]
  lazy val rightsFields2 = new mutable.HashMap[String, AbstractFieldBuilder]()
  lazy val dataVaultFields = new ListBuffer[MetadataField]
  lazy val dataVaultFields2 = new mutable.HashMap[String, AbstractFieldBuilder]()

  def toDataverseDataset(ddm: Node, contactData: CompoundField, contactData2: List[JsonObject], vaultMetadata: VaultMetadata): Try[Dataset] = Try {
    // Please, keep ordered by order in Dataverse UI as much as possible (note, if display-on-create is not set for all fields, some may be hidden initally)

    val titles = ddm \ "profile" \ "title"
    if (titles.isEmpty) throw MissingRequiredFieldException("title")

    val alternativeTitles = (ddm \ "dcmiMetadata" \ "title") ++ (ddm \ "dcmiMetadata" \ "alternative")

    // Citation
    addPrimitiveFieldSingleValue(citationFields, TITLE, titles.head)
    addPrimitiveFieldSingleValue2(citationFields2, TITLE, titles.head)

    addPrimitiveFieldSingleValue(citationFields, ALTERNATIVE_TITLE, alternativeTitles)
    addPrimitiveFieldSingleValue2(citationFields2, ALTERNATIVE_TITLE, alternativeTitles)

    addCompoundFieldMultipleValues(citationFields, OTHER_ID, ddm \ "dcmiMetadata" \ "isFormatOf", IsFormatOf toOtherIdValueObject)
    addCompoundFieldMultipleValues2(citationFields2, OTHER_ID, ddm \ "dcmiMetadata" \ "isFormatOf", IsFormatOf toOtherIdValueObject)

    addCompoundFieldMultipleValues(citationFields, AUTHOR, ddm \ "profile" \ "creatorDetails" \ "author", DcxDaiAuthor toAuthorValueObject)
    addCompoundFieldMultipleValues2(citationFields2, AUTHOR, ddm \ "profile" \ "creatorDetails" \ "author", DcxDaiAuthor toAuthorValueObject)

    addCompoundFieldMultipleValues(citationFields, AUTHOR, ddm \ "profile" \ "creatorDetails" \ "organization", DcxDaiOrganization toAuthorValueObject)
    addCompoundFieldMultipleValues2(citationFields2, AUTHOR, ddm \ "profile" \ "creatorDetails" \ "organization", DcxDaiOrganization toAuthorValueObject)

    addCompoundFieldMultipleValues(citationFields, AUTHOR, ddm \ "profile" \ "creator", Creator toAuthorValueObject)
    addCompoundFieldMultipleValues2(citationFields2, AUTHOR, ddm \ "profile" \ "creator", Creator toAuthorValueObject)

    citationFields.append(contactData)
    addCompoundFieldMultipleValues2(citationFields2, DATASET_CONTACT, contactData2)

    addCompoundFieldMultipleValues(citationFields, DESCRIPTION, ddm \ "profile" \ "description", Description toDescriptionValueObject)
    addCompoundFieldMultipleValues2(citationFields2, DESCRIPTION, ddm \ "profile" \ "description", Description toDescriptionValueObject)

    addCompoundFieldMultipleValues(citationFields, DESCRIPTION, if (alternativeTitles.isEmpty) NodeSeq.Empty
                                                                else alternativeTitles.tail, Description toDescriptionValueObject)
    addCompoundFieldMultipleValues2(citationFields2, DESCRIPTION, if (alternativeTitles.isEmpty) NodeSeq.Empty
                                                                  else alternativeTitles.tail, Description toDescriptionValueObject)

    // TODO: add languages that cannot be mapped to Dataverse language terms.

    val audience = ddm \ "profile" \ "audience"
    if (audience.isEmpty) throw MissingRequiredFieldException(SUBJECT)

    addCvFieldMultipleValues(citationFields, SUBJECT, ddm \ "profile" \ "audience", Audience toCitationBlockSubject)
    addCvFieldMultipleValues2(citationFields2, SUBJECT, ddm \ "profile" \ "audience", Audience toCitationBlockSubject)

    addCvFieldMultipleValues(citationFields, LANGUAGE, ddm \ "dcmiMetadata" \ "language", Language.toCitationBlockLanguage(isoToDataverseLanguage))
    addCvFieldMultipleValues2(citationFields2, LANGUAGE, ddm \ "dcmiMetadata" \ "language", Language.toCitationBlockLanguage(isoToDataverseLanguage))

    addPrimitiveFieldSingleValue(citationFields, PRODUCTION_DATE, ddm \ "profile" \ "created", DateTypeElement toYearMonthDayFormat)
    addPrimitiveFieldSingleValue2(citationFields2, PRODUCTION_DATE, ddm \ "profile" \ "created", DateTypeElement toYearMonthDayFormat)

    addCompoundFieldMultipleValues(citationFields, CONTRIBUTOR, ddm \ "dcmiMetadata" \ "contributorDetails" \ "author", DcxDaiAuthor toContributorValueObject)
    addCompoundFieldMultipleValues2(citationFields2, CONTRIBUTOR, ddm \ "dcmiMetadata" \ "contributorDetails" \ "author", DcxDaiAuthor toContributorValueObject)

    addCompoundFieldMultipleValues(citationFields, CONTRIBUTOR, ddm \ "dcmiMetadata" \ "contributorDetails" \ "organization", DcxDaiOrganization toContributorValueObject)
    addCompoundFieldMultipleValues2(citationFields2, CONTRIBUTOR, ddm \ "dcmiMetadata" \ "contributorDetails" \ "organization", DcxDaiOrganization toContributorValueObject)

    addPrimitiveFieldSingleValue(citationFields, DISTRIBUTION_DATE, ddm \ "profile" \ "available", DateTypeElement toYearMonthDayFormat)
    addPrimitiveFieldSingleValue2(citationFields2, DISTRIBUTION_DATE, ddm \ "profile" \ "available", DateTypeElement toYearMonthDayFormat)

    addPrimitiveFieldMultipleValues(citationFields, DATA_SOURCES, ddm \ "dcmiMetadata" \ "source")
    addPrimitiveFieldMultipleValues2(citationFields2, DATA_SOURCES, ddm \ "dcmiMetadata" \ "source")

    // Archaeology specific
    addPrimitiveFieldMultipleValues(archaeologySpecificFields, ARCHIS_ZAAK_ID, ddm \ "dcmiMetadata" \ "identifier", IsFormatOf toArchisZaakId)
    addPrimitiveFieldMultipleValues2(archaeologySpecificFields2, ARCHIS_ZAAK_ID, ddm \ "dcmiMetadata" \ "identifier", IsFormatOf toArchisZaakId)

    addCompoundFieldMultipleValues(archaeologySpecificFields, ABR_RAPPORT_TYPE, (ddm \ "dcmiMetadata" \ "reportNumber").filter(AbrReportType isAbrReportType), AbrReportType toAbrRapportType)
    addCompoundFieldMultipleValues2(archaeologySpecificFields2, ABR_RAPPORT_TYPE, (ddm \ "dcmiMetadata" \ "reportNumber").filter(AbrReportType isAbrReportType), AbrReportType toAbrRapportType)

    addPrimitiveFieldMultipleValues(archaeologySpecificFields, ABR_RAPPORT_NUMMER, ddm \ "dcmiMetadata" \ "reportNumber")
    addPrimitiveFieldMultipleValues2(archaeologySpecificFields2, ABR_RAPPORT_NUMMER, ddm \ "dcmiMetadata" \ "reportNumber")

    addCompoundFieldMultipleValues(archaeologySpecificFields, ABR_VERWERVINGSWIJZE, (ddm \ "dcmiMetadata" \ "acquisitionMethod").filter(AbrAcquisitionMethod isAbrVerwervingswijze), AbrAcquisitionMethod toVerwervingswijze)
    addCompoundFieldMultipleValues2(archaeologySpecificFields2, ABR_VERWERVINGSWIJZE, (ddm \ "dcmiMetadata" \ "acquisitionMethod").filter(AbrAcquisitionMethod isAbrVerwervingswijze), AbrAcquisitionMethod toVerwervingswijze)

    addCompoundFieldMultipleValues(archaeologySpecificFields, ABR_COMPLEX, (ddm \ "dcmiMetadata" \ "subject").filter(SubjectAbr isAbrComplex), SubjectAbr toAbrComplex)
    addCompoundFieldMultipleValues2(archaeologySpecificFields2, ABR_COMPLEX, (ddm \ "dcmiMetadata" \ "subject").filter(SubjectAbr isAbrComplex), SubjectAbr toAbrComplex)

    addCompoundFieldMultipleValues(archaeologySpecificFields, ABR_PERIOD, (ddm \ "dcmiMetadata" \ "temporal").filter(TemporalAbr isAbrPeriod), TemporalAbr toAbrPeriod)
    addCompoundFieldMultipleValues2(archaeologySpecificFields2, ABR_PERIOD, (ddm \ "dcmiMetadata" \ "temporal").filter(TemporalAbr isAbrPeriod), TemporalAbr toAbrPeriod)

    // Temporal and spatial coverage
    addCompoundFieldMultipleValues(temporalSpatialFields, SPATIAL_POINT, ddm \ "dcmiMetadata" \ "spatial" \ "Point", SpatialPoint toEasyTsmSpatialPointValueObject)
    addCompoundFieldMultipleValues2(temporalSpatialFields2, SPATIAL_POINT, ddm \ "dcmiMetadata" \ "spatial" \ "Point", SpatialPoint toEasyTsmSpatialPointValueObject)

    addCompoundFieldMultipleValues(temporalSpatialFields, SPATIAL_BOX, ddm \ "dcmiMetadata" \ "spatial" \ "boundedBy", SpatialBox toEasyTsmSpatialBoxValueObject)
    addCompoundFieldMultipleValues2(temporalSpatialFields2, SPATIAL_BOX, ddm \ "dcmiMetadata" \ "spatial" \ "boundedBy", SpatialBox toEasyTsmSpatialBoxValueObject)

    // Rights
    val rightsHolder = ddm \ "dcmiMetadata" \ "rightsHolder"
    if (rightsHolder.isEmpty) throw MissingRequiredFieldException(RIGHTS_HOLDER)
    addPrimitiveFieldMultipleValues(rightsFields, RIGHTS_HOLDER, ddm \ "dcmiMetadata" \ "rightsHolder", AnyElement toText)
    addPrimitiveFieldMultipleValues2(rightsFields2, RIGHTS_HOLDER, ddm \ "dcmiMetadata" \ "rightsHolder", AnyElement toText)

    // Data vault
    addVaultValue(dataVaultFields, BAG_ID, vaultMetadata.dataverseBagId)
    addPrimitiveFieldSingleValue2(dataVaultFields2, BAG_ID, Option(vaultMetadata.dataverseBagId))

    addVaultValue(dataVaultFields, NBN, vaultMetadata.dataverseNbn)
    addPrimitiveFieldSingleValue2(dataVaultFields2, NBN, Option(vaultMetadata.dataverseNbn))

    addVaultValue(dataVaultFields, DANS_OTHER_ID, vaultMetadata.dataverseOtherId)
    addPrimitiveFieldSingleValue2(dataVaultFields2, DANS_OTHER_ID, Option(vaultMetadata.dataverseOtherId))

    addVaultValue(dataVaultFields, DANS_OTHER_ID_VERSION, vaultMetadata.dataverseOtherIdVersion)
    addPrimitiveFieldSingleValue2(dataVaultFields2, DANS_OTHER_ID_VERSION, Option(vaultMetadata.dataverseOtherIdVersion))

    addVaultValue(dataVaultFields, SWORD_TOKEN, vaultMetadata.dataverseSwordToken)
    addPrimitiveFieldSingleValue2(dataVaultFields2, SWORD_TOKEN, Option(vaultMetadata.dataverseSwordToken))

//    assembleDataverseDataset()
    assembleDataverseDataset2()
  }

  private def assembleDataverseDataset(): Dataset = {
    val versionMap = mutable.Map[String, MetadataBlock]()
    addMetadataBlock(versionMap, "citation", "Citation Metadata", citationFields)
    addMetadataBlock(versionMap, "archaeologyMetadata", "Archaeology-Specific Metadata", archaeologySpecificFields)
    addMetadataBlock(versionMap, "temporal-spatial", "Temporal and Spatial Coverage", temporalSpatialFields)
    addMetadataBlock(versionMap, "dansRights", "Rights Metadata", rightsFields)
    addMetadataBlock(versionMap, "dataVault", "Data Vault Metadata", dataVaultFields)
    val datasetVersion = DatasetVersion(metadataBlocks = versionMap.toMap)
    Dataset(datasetVersion)
  }

  private def assembleDataverseDataset2(): Dataset = {
    val versionMap = mutable.Map[String, MetadataBlock]()
    addMetadataBlock2(versionMap, "citation", "Citation Metadata", citationFields2)
    addMetadataBlock2(versionMap, "archaeologyMetadata", "Archaeology-Specific Metadata", archaeologySpecificFields2)
    addMetadataBlock2(versionMap, "temporal-spatial", "Temporal and Spatial Coverage", temporalSpatialFields2)
    addMetadataBlock2(versionMap, "dansRights", "Rights Metadata", rightsFields2)
    addMetadataBlock2(versionMap, "dataVault", "Data Vault Metadata", dataVaultFields2)
    val datasetVersion = DatasetVersion(metadataBlocks = versionMap.toMap)
    Dataset(datasetVersion)
  }


  private def addPrimitiveFieldSingleValue(metadataBlockFields: ListBuffer[MetadataField], name: String, sourceNodes: NodeSeq, nodeTransformer: Node => Option[String] = AnyElement toText): Unit = {
    sourceNodes
      .map(nodeTransformer)
      .filter(_.isDefined)
      .map(_.get)
      .take(1)
      .foreach(v => metadataBlockFields += PrimitiveSingleValueField(name, v))
  }

  private def addPrimitiveFieldSingleValue2(metadataBlockFields: mutable.HashMap[String, AbstractFieldBuilder], name: String, sourceNodes: NodeSeq, nodeTransformer: Node => Option[String] = AnyElement toText): Unit = {
    sourceNodes
      .map(nodeTransformer)
      .filter(_.isDefined)
      .map(_.get)
      .take(1)
      .foreach(v => {
        metadataBlockFields.getOrElseUpdate(name, new PrimitiveFieldBuilder(name, multipleValues = false)) match {
          case b: PrimitiveFieldBuilder => b.addValue(v)
        }
      })
  }

  private def addPrimitiveFieldSingleValue2(metadataBlockFields: mutable.HashMap[String, AbstractFieldBuilder], name: String, value: Option[String]): Unit = {
    value.foreach { v =>
      metadataBlockFields.getOrElseUpdate(name, new PrimitiveFieldBuilder(name, multipleValues = false)) match {
        case b: PrimitiveFieldBuilder => b.addValue(v)
      }
    }
  }

  private def addPrimitiveFieldMultipleValues(metadataBlockFields: ListBuffer[MetadataField], name: String, sourceNodes: NodeSeq, nodeTransformer: Node => Option[String] = AnyElement toText): Unit = {
    val values = sourceNodes.map(nodeTransformer).filter(_.isDefined).map(_.get).toList
    if (values.nonEmpty) {
      metadataBlockFields += PrimitiveMultipleValueField(name, values)
    }
  }

  private def addPrimitiveFieldMultipleValues2(metadataBlockFields: mutable.HashMap[String, AbstractFieldBuilder], name: String, sourceNodes: NodeSeq, nodeTransformer: Node => Option[String] = AnyElement toText): Unit = {
    val values = sourceNodes.map(nodeTransformer).filter(_.isDefined).map(_.get).toList
    values.foreach { v =>
      metadataBlockFields.getOrElseUpdate(name, new PrimitiveFieldBuilder(name, multipleValues = true)) match {
        case b: PrimitiveFieldBuilder => b.addValue(v)
      }
    }
  }

  private def addCvFieldMultipleValues(metadataBlockFields: ListBuffer[MetadataField], name: String, sourceNodes: NodeSeq, nodeTransformer: Node => Option[String]): Unit = {
    val values = sourceNodes.map(nodeTransformer).filter(_.isDefined).map(_.get).toList
    if (values.nonEmpty) {
      metadataBlockFields += ControlledMultipleValueField(name, values)
    }
  }

  private def addCvFieldMultipleValues2(metadataBlockFields: mutable.HashMap[String, AbstractFieldBuilder], name: String, sourceNodes: NodeSeq, nodeTransformer: Node => Option[String]): Unit = {
    val values = sourceNodes.map(nodeTransformer).filter(_.isDefined).map(_.get).toList
    metadataBlockFields.getOrElseUpdate(name, new CvFieldBuilder(name)) match {
      case cfb: CvFieldBuilder => values.foreach(cfb.addValue)
      case _ => throw new IllegalArgumentException("Trying to add non-controlled-vocabulary value(s) to controlled vocabulary field")
    }
  }

  private def addCompoundFieldMultipleValues(metadataBlockFields: ListBuffer[MetadataField], name: String, sourceNodes: NodeSeq, nodeTransformer: Node => JsonObject): Unit = {
    val valueObjects = new ListBuffer[JsonObject]()
    sourceNodes.foreach(e => valueObjects += nodeTransformer(e))
    if (valueObjects.nonEmpty) {
      metadataBlockFields += CompoundField(name, valueObjects.toList)
    }
  }

  private def addCompoundFieldMultipleValues2(fields: mutable.HashMap[String, AbstractFieldBuilder], name: String, sourceNodes: NodeSeq, nodeTransformer: Node => JsonObject): Unit = {
    val valueObjects = new ListBuffer[JsonObject]()
    sourceNodes.foreach(e => valueObjects += nodeTransformer(e))
    fields.getOrElseUpdate(name, new CompoundFieldBuilder(name)) match {
      case cfb: CompoundFieldBuilder => valueObjects.foreach(cfb.addValue)
      case _ => throw new IllegalArgumentException("Trying to add non-compound value(s) to compound field")
    }
  }

  private def addCompoundFieldMultipleValues2(fields: mutable.HashMap[String, AbstractFieldBuilder], name: String, valueObjects: List[JsonObject]): Unit = {
    fields.getOrElseUpdate(name, new CompoundFieldBuilder(name)) match {
      case cfb: CompoundFieldBuilder => valueObjects.foreach(cfb.addValue)
      case _ => throw new IllegalArgumentException("Trying to add non-compound value(s) to compound field")
    }
  }

  private def addCompoundFieldWithControlledVocabulary(metadataBlockFields: ListBuffer[MetadataField], name: String, sourceNodes: NodeSeq, nodeTransformer: Node => Option[JsonObject]): Unit = {
    val valueObjects = new ListBuffer[JsonObject]()
    sourceNodes.foreach({
      e =>
        nodeTransformer(e) match {
          case Some(jsonObject: JsonObject) => valueObjects += jsonObject
          case None => valueObjects
        }
    })
    if (valueObjects.nonEmpty) {
      metadataBlockFields += CompoundField(name, valueObjects.toList)
    }
  }

  private def addVaultValue(metadataBlockFields: ListBuffer[MetadataField], name: String, value: String): Unit = {
    if (value.nonEmpty) {
      metadataBlockFields += PrimitiveSingleValueField(name, value)
    }
  }

  private def addMetadataBlock(versionMap: mutable.Map[String, MetadataBlock], blockId: String, blockDisplayName: String, fields: ListBuffer[MetadataField]): Unit = {
    if (fields.nonEmpty) {
      versionMap.put(blockId, MetadataBlock(blockDisplayName, fields.toList))
    }
  }

  private def addMetadataBlock2(versionMap: mutable.Map[String, MetadataBlock], blockId: String, blockDisplayName: String, fields: mutable.HashMap[String, AbstractFieldBuilder]): Unit = {
    if (fields.nonEmpty) {
      versionMap.put(blockId, MetadataBlock(blockDisplayName, fields.values.map(_.build()).filter(_.isDefined).map(_.get).toList))
    }
  }
}
