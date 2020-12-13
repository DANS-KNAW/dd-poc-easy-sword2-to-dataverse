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

import nl.knaw.dans.easy.dd2d.mapping._
import nl.knaw.dans.lib.dataverse.model.dataset.{ CompoundField, ControlledMultipleValueField, Dataset, DatasetVersion, MetadataBlock, MetadataField, PrimitiveMultipleValueField, PrimitiveSingleValueField }

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.language.postfixOps
import scala.util.Try
import scala.xml.{ Node, NodeSeq }

/**
 * Maps DANS Dataset Metadata to Dataverse JSON.
 */
// TODO: Rename if we also need to take elements from EMD
class DepositToDataverseMapper() extends BlockCitation with BlockBasicInformation with BlockArchaeologySpecific with BlockTemporalAndSpatial with BlockContentTypeAndFileFormat with BlockDataVaultMetadata {
  lazy val citationFields = new ListBuffer[MetadataField]
  lazy val basicInformationFields = new ListBuffer[MetadataField]
  lazy val archaeologySpecificFields = new ListBuffer[MetadataField]
  lazy val temporalSpatialFields = new ListBuffer[MetadataField]
  lazy val dataVaultFields = new ListBuffer[MetadataField]
  lazy val contentTypeAndFileFormatFields = new ListBuffer[MetadataField]

  def toDataverseDataset(ddm: Node, vaultMetadata: VaultMetadata): Try[Dataset] = Try {
    // Please keep ordered by order in Dataverse UI as much as possible

    // TODO: if a single value is expected, the first encountered will be used; is this OK? Add checks on multiplicity before processing?

    // Citation
    addPrimitiveFieldSingleValue(citationFields, TITLE, ddm \ "profile" \ "title")
    addCompoundFieldMultipleValues(citationFields, OTHER_ID, ddm \ "dcmiMetadata" \ "isFormatOf", IsFormatOf toOtherIdValueObject)
    addCompoundFieldMultipleValues(citationFields, DESCRIPTION, ddm \ "profile" \ "description", Description toDescriptionValueObject)
    addCompoundFieldMultipleValues(citationFields, AUTHOR, ddm \ "profile" \ "creatorDetails" \ "author", DcxDaiAuthor toAuthorValueObject)
    addCompoundFieldMultipleValues(citationFields, AUTHOR, ddm \ "profile" \ "creatorDetails" \ "organization", DcxDaiOrganization toAuthorValueObject)

    // TODO: creator unstructured
    addPrimitiveFieldSingleValue(citationFields, PRODUCTION_DATE, ddm \ "profile" \ "created", DateTypeElement toYearMonthDayFormat)
    addPrimitiveFieldSingleValue(citationFields, DISTRIBUTION_DATE, ddm \ "profile" \ "available", DateTypeElement toYearMonthDayFormat)
    addCvFieldMultipleValues(citationFields, SUBJECT, ddm \ "profile" \ "audience", Audience toCitationBlockSubject)
    addCvFieldMultipleValues(citationFields, LANGUAGE, ddm \ "dcmiMetadata" \ "language", Language toCitationBlockLanguage)
    addPrimitiveFieldSingleValue(citationFields, ALTERNATIVE_TITLE, ddm \ "dcmiMetadata" \ "alternative")
    addPrimitiveFieldMultipleValues(citationFields, DATA_SOURCES, ddm \ "dcmiMetadata" \ "source")
    addCompoundFieldMultipleValues(citationFields, CONTRIBUTOR, ddm \ "dcmiMetadata" \ "contributorDetails" \ "author", DcxDaiAuthor toContributorValueObject)
    addCompoundFieldMultipleValues(citationFields, CONTRIBUTOR, ddm \ "dcmiMetadata" \ "contributorDetails" \ "organization", DcxDaiOrganization toContributorValueObject)
    // TODO: contributor unstructured

    // Basic information
    addCompoundFieldWithControlledVocabulary(basicInformationFields, LANGUAGE_OF_METADATA_CV, ddm, Language toBasicInformationLanguageOfMetadata)
    addCompoundFieldMultipleValues(basicInformationFields, RELATED_ID, (ddm \ "dcmiMetadata" \ "_").filter(Relation isRelation).filter(Relation isRelatedIdentifier), Relation toRelatedIdentifierValueObject)
    addCompoundFieldMultipleValues(basicInformationFields, RELATED_ID_URL, (ddm \ "dcmiMetadata" \ "_").filter(Relation isRelation).filterNot(Relation isRelatedIdentifier), Relation toRelatedUrlValueObject)
    addPrimitiveFieldMultipleValues(basicInformationFields, LANGUAGE_OF_FILES, ddm \ "dcmiMetadata" \ "language")
    addCompoundFieldWithControlledVocabulary(basicInformationFields, LANGUAGE_OF_FILES_CV, (ddm \\ "language").filter(Language isISOLanguage), Language toBasicInformationBlockLanguageOfFiles)
    addCompoundFieldMultipleValues(basicInformationFields, DATE, (ddm \ "dcmiMetadata" \ "_").filter(DateTypeElement isDate).filter(DateTypeElement hasW3CFormat), DateTypeElement toBasicInfoFormattedDateValueObject)
    addCompoundFieldMultipleValues(basicInformationFields, DATE_FREE_FORMAT, (ddm \ "dcmiMetadata" \ "_").filter(DateTypeElement isDate).filterNot(DateTypeElement hasW3CFormat), DateTypeElement toBasicInfoFreeDateValue)
    addCompoundFieldWithControlledVocabulary(basicInformationFields, SUBJECT_CV, ddm \ "profile" \ "audience", Audience toBasicInformationBlockSubjectCv)

    // Archaeology specific
    addPrimitiveFieldMultipleValues(archaeologySpecificFields, ARCHIS_ZAAK_ID, ddm \ "dcmiMetadata" \ "identifier", IsFormatOf toArchisZaakId)
    addCompoundFieldWithControlledVocabulary(archaeologySpecificFields, ABR_SUBJECT, (ddm \\ "subject").filter(SubjectAbr isAbrComplex), SubjectAbr toSubjectAbrObject)
    addCompoundFieldWithControlledVocabulary(archaeologySpecificFields, ABR_PERIOD, (ddm \\ "temporal").filter(TemporalAbr isTemporalAbr), TemporalAbr toTemporalAbr)

    // Temporal and spatial coverage
    addCompoundFieldMultipleValues(temporalSpatialFields, SPATIAL_POINT, ddm \ "dcmiMetadata" \ "spatial" \ "Point", SpatialPoint toEasyTsmSpatialPointValueObject)
    addCompoundFieldMultipleValues(temporalSpatialFields, SPATIAL_BOX, ddm \ "dcmiMetadata" \ "spatial" \ "boundedBy", SpatialBox toEasyTsmSpatialBoxValueObject)

    //content type and file format
    addCompoundFieldWithControlledVocabulary(contentTypeAndFileFormatFields, CONTENT_TYPE_CV, ddm \\ "type", Type toContentTypeAndFileFormatBlockType)
    addCompoundFieldWithControlledVocabulary(contentTypeAndFileFormatFields, FORMAT_CV, ddm \\ "format", Format toContentTypeAndFileFormatBlockFormat)

    // Data vault
    addVaultValue(dataVaultFields, DATAVERSE_PID, vaultMetadata.dataversePid)
    addVaultValue(dataVaultFields, BAG_ID, vaultMetadata.dataverseBagId)
    addVaultValue(dataVaultFields, NBN, vaultMetadata.dataverseNbn)
    addVaultValue(dataVaultFields, DANS_OTHER_ID, vaultMetadata.dataverseOtherId)
    addVaultValue(dataVaultFields, DANS_OTHER_ID_VERSION, vaultMetadata.dataverseOtherIdVersion)
    addVaultValue(dataVaultFields, SWORD_TOKEN, vaultMetadata.dataverseSwordToken)

    assembleDataverseDataset()
  }

  private def assembleDataverseDataset(): Dataset = {
    val versionMap = mutable.Map[String, MetadataBlock]()
    addMetadataBlock(versionMap, "citation", "Citation Metadata", citationFields)
    addMetadataBlock(versionMap, "basicInformation", "Basic Information", basicInformationFields)
    addMetadataBlock(versionMap, "archaeologyMetadata", "Archaeology-Specific Metadata", archaeologySpecificFields)
    addMetadataBlock(versionMap, "temporal-spatial", "Temporal and Spatial Coverage", temporalSpatialFields)
    addMetadataBlock(versionMap, "dansContentTypeAndFileFormat", "Content Type and File Format", contentTypeAndFileFormatFields)
    addMetadataBlock(versionMap, "dataVault", "Data Vault Metadata", dataVaultFields)
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

  private def addPrimitiveFieldMultipleValues(metadataBlockFields: ListBuffer[MetadataField], name: String, sourceNodes: NodeSeq, nodeTransformer: Node => Option[String] = AnyElement toText): Unit = {
    val values = sourceNodes.map(nodeTransformer).filter(_.isDefined).map(_.get).toList
    if (values.nonEmpty) {
      metadataBlockFields += PrimitiveMultipleValueField(name, values)
    }
  }

  private def addCvFieldMultipleValues(metadataBlockFields: ListBuffer[MetadataField], name: String, sourceNodes: NodeSeq, nodeTransformer: Node => Option[String]): Unit = {
    val values = sourceNodes.map(nodeTransformer).filter(_.isDefined).map(_.get).toList
    if (values.nonEmpty) {
      metadataBlockFields += ControlledMultipleValueField(name, values)
    }
  }

  private def addCompoundFieldMultipleValues(metadataBlockFields: ListBuffer[MetadataField], name: String, sourceNodes: NodeSeq, nodeTransformer: Node => JsonObject): Unit = {
    val valueObjects = new ListBuffer[JsonObject]()
    sourceNodes.foreach(e => valueObjects += nodeTransformer(e))
    if (valueObjects.nonEmpty) {
      metadataBlockFields += CompoundField(name, valueObjects.toList)
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
}
