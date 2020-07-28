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

import nl.knaw.dans.easy.s2d.dataverse.DataverseInstance
import nl.knaw.dans.easy.s2d.dataverse.json.{ CompoundField, Field, PrimitiveFieldMultipleValues, PrimitiveFieldSingleValue, _ }
import nl.knaw.dans.easy.s2d.queue.Task
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.json4s.native.Serialization
import org.json4s.{ DefaultFormats, Formats }

import scala.collection.mutable.ListBuffer
import scala.util.Try
import scala.xml.{ Elem, Node, XML }

/**
 * Checks one deposit and then ingests it into Dataverse.
 *
 * @param deposit     the deposit to ingest
 * @param dataverse   the Dataverse instance to ingest in
 * @param jsonFormats implicit necessary for pretty-printing JSON
 */
case class DepositIngestTask(deposit: Deposit, dataverse: DataverseInstance)(implicit jsonFormats: Formats) extends Task with DebugEnhancedLogging {
  trace(deposit, dataverse)

  implicit val format = DefaultFormats

  override def run(): Try[Unit] = Try {
    trace(())
    debug(s"Ingesting $deposit into Dataverse")

    // TODO: validate: is this a deposit can does it contain a bag that conforms to DANS BagIt Profile? (call easy-validate-dans-bag)

    val citationFields = new ListBuffer[Field]
    val access_and_LicenceFields = new ListBuffer[Field]
    val depositAgreementFields = new ListBuffer[Field]
    val basicInformationFields = new ListBuffer[Field]


    //todo find better solution
    val xml = deposit.tryDdm.getOrElse(throw new Exception("xml could not be parsed"))
    val profile = (xml \ "profile").head.nonEmptyChildren
    val dcmi = (xml \ "dcmiMetadata").head.nonEmptyChildren
    //join profile and dcmi parts for iteration
    val ddm = XML.loadString("<ddm>" + profile.union(dcmi).toString() + "</ddm>")

    mapFields(ddm)

    def mapFields(node: Node): Unit = {

      // Todo Which title: DV allows only one
      val title = (node \\ "title").head.text
      addPrimitiveFieldToMetadataBlock("title", false, "primitive", Some(title), None, "citation")

      mapToPrimitiveFieldsSingleValue(node)
      mapToPrimitiveFieldsMultipleValues(node)
      mapToCompoundFields(node)
    }

    def mapToPrimitiveFieldsSingleValue(node: Node): Unit = {
      node.nonEmptyChildren.foreach {
        case e @ Elem("dcterms", "alternative", _, _, _) => addPrimitiveFieldToMetadataBlock("alternativeTitle", multi = false, "primitive", Some(e.text), None, "citation")
        //case node @ _ if node.label.equals("creatorDetails") => addCreator(node)
        case e @ Elem("ddm", "created", _, _, _) => addPrimitiveFieldToMetadataBlock("productionDate", multi = false, "primitive", Some(e.text), None, "citation")
        case e @ Elem("ddm", "accessRights", _, _, _) => addPrimitiveFieldToMetadataBlock("accessrights", multi = false, "controlledVocabulary", Some(e.text), None, "access_and_licence")
        //Todo get the correct element
        case e @ Elem(_, "instructionalMethod", _, _, _) => addPrimitiveFieldToMetadataBlock("accept", multi = false, "controlledVocabulary", Some(e.text), None, "depositAgreement")
        //welk element is language of description? dcterms?
        case e @ Elem("dcterms", "language", _, _, _) => addPrimitiveFieldToMetadataBlock("languageofmetadata", multi = false, "controlledVocabulary", Some(e.text), None, "basicInformation")
        case _ => ()
      }
    }

    def mapToPrimitiveFieldsMultipleValues(node: Node): Unit = {
      // Todo create mapping for EASY Dropdown values (d2700 etc.) to DV values
      val audience = (node \\ "audience").filter(e => !e.text.equals("")).map(_.text).toList
      addPrimitiveFieldToMetadataBlock("subject", multi = true, "controlledVocabulary", None, Some(audience), "citation")

      val language = (node \\ "language").filter(e => !e.text.equals("")).map(_.text).toList
      addPrimitiveFieldToMetadataBlock("language", multi = true, "controlledVocabulary", None, Some(language), "citation")

      val source = (node \\ "source").filter(e => !e.text.equals("")).map(_.text).toList
      addPrimitiveFieldToMetadataBlock("dataSources", multi = true, "primitive", None, Some(source), "citation")

      /// Todo Fix .tsv file. Should be Controlled Vocabulary
      //      val languageOfFiles = (node \\ "language").filter(e => !e.text.equals("")).map(_.text).toList
      //      addPrimitiveFieldToMetadataBlock("languageFiles", multi = true, primitive, None, Some(languageOfFiles), "basicInformation")
    }

    def mapToCompoundFields(node: Node): Unit = {
      addCreator(node)
      addContributors(node)
      addAlternativeIdentifier(node)
      //addDate(node)
      //addDateFree(node)
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
        case _ => new ListBuffer[Field]
      }
    }

    def getAuthorName(author: Node): String = {
      var authorName = new ListBuffer[String]
      author.nonEmptyChildren.foreach {
        case e @ Elem(str, "titles", data, binding, node) => authorName += e.text
        case e @ Elem(str, "initials", data, binding, node) => authorName += e.text
        case e @ Elem(str, "surname", data, binding, node) => authorName += e.text
        case _ => ()
      }
      authorName.mkString(", ")
    }

    def addCreator(cf: Node): Unit = {
      val author = cf \\ "author"
      var subFields = collection.mutable.Map[String, Field]()
      subFields += ("authorName" -> PrimitiveFieldSingleValue("authorName", multiple = false, "primitive", getAuthorName(author.head)))

      author.head.child.foreach {
        case node @ _ if node.label.equals("organization") => subFields += ("authorAffiliation" -> PrimitiveFieldSingleValue("authorAffiliation", false, "primitive", (node \ "name").head.text))
        case e @ Elem("dcx-dai", "DAI", _, _, _) => {
          subFields += ("authorIdentifierScheme" -> PrimitiveFieldSingleValue("authorIdentifierScheme", multiple = false, "controlledVocabulary", e.label))
          subFields += ("authorIdentifier" -> PrimitiveFieldSingleValue("authorIdentifier", multiple = false, "primitive", e.text))
        }
        case e @ Elem("dcx-dai", "ISNI", _, _, _) => {
          subFields += ("authorIdentifierScheme" -> PrimitiveFieldSingleValue("authorIdentifierScheme", multiple = false, "controlledVocabulary", e.label))
          subFields += ("authorIdentifier" -> PrimitiveFieldSingleValue("authorIdentifier", multiple = false, "primitive", e.text))
        }
        case e @ Elem("dcx-dai", "ORCID", _, _, _) => {
          subFields += ("authorIdentifierScheme" -> PrimitiveFieldSingleValue("authorIdentifierScheme", multiple = false, "controlledVocabulary", e.label))
          subFields += ("authorIdentifier" -> PrimitiveFieldSingleValue("authorIdentifier", multiple = false, "primitive", e.text))
        }
        case _ => ()
      }
      addCompoundFieldToMetadataBlock("citation", CompoundField("author", multiple = true, "compound", List(subFields.toMap)))
    }

    def addContributors(node: Node): Unit = {
      val objectList = new ListBuffer[Map[String, Field]]()
      (node \\ "contributorDetails").map(contributorNode => {
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

    // Todo How to get idType? From namespace?
    def addAlternativeIdentifier(node: Node): Unit = {
      val objectList = new ListBuffer[Map[String, Field]]()
      (node \\ "identifier").foreach(contributorNode => {
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

    def addDate(node: Node): Unit = {
      val objectList = new ListBuffer[Map[String, Field]]()
    }

    val citationBlock = MetadataBlock("Citation Metadata", citationFields.toList)
    val access_and_licenseBlock = MetadataBlock("Access and License", access_and_LicenceFields.toList)
    val depositAgreementBlock = MetadataBlock("Deposit Agreement", depositAgreementFields.toList)
    val basicInformation = MetadataBlock("Basic Information", basicInformationFields.toList)
    val datasetVersion = DatasetVersion(Map("citation" -> citationBlock, "basicInformation" -> basicInformation, "depositAgreement" -> depositAgreementBlock, "access-and-license" -> access_and_licenseBlock))
    val dataverseDataset = DataverseDataset(datasetVersion)

    println(Serialization.writePretty(dataverseDataset))

    dataverse.dataverse("root").createDataset(Serialization.writePretty(dataverseDataset)).map(_ => ())
  }
}






