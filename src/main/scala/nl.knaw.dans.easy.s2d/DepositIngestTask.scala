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
import org.json4s.{ DefaultFormats, Formats }
import org.json4s.native.Serialization

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

  override def run(): Try[Unit] = {
    trace(())
    debug(s"Ingesting $deposit into Dataverse")

    // TODO: validate: is this a deposit can does it contain a bag that conforms to DANS BagIt Profile? (call easy-validate-dans-bag)

    var citationfields = new ListBuffer[Field]
    var access_and_LicenceFields = new ListBuffer[Field]
    var depositAgreementFields = new ListBuffer[Field]

    //EASY XML
    val xml = XML.loadFile("/Users/chrismuller/Development/Git/Github/DANS-KNAW/dd-easy-sword2-to-dataverse/data/inbox/valid-easy-submitted/example-bag-medium/metadata/dataset1.xml")
    //val ddm = deposit.tryDdm.getOrElse(Failure).asInstanceOf[Node]
    val profile = xml \ "profile"
    val dcmiMetadata = xml \ "dcmiMetadata"
    //join profile and dcmi parts for iteration
    val ddm = profile ++ dcmiMetadata

    mapFields(ddm.head)
    mapPrimitiveFieldsMultiple(ddm.head)

    def mapFields(node: Node): Unit = {
      val title = (node \\ "title").head.text
      addPrimitiveFieldToMetadataBlock("title", false, "primitive", Some(title), None, "citation")
      mapPrimitiveFieldsMultiple(node)
      node.child.foreach {
        case e @ Elem("dcterms", "alternative", _, _, _) => addPrimitiveFieldToMetadataBlock("alternativeTitle", multi = false, "primitive", Some(e.text), None, "citation")
        case node @ _ if node.label.equals("creatorDetails") => addCreator(node)
        case e @ Elem("ddm", "created", _, _, _) => addPrimitiveFieldToMetadataBlock("productionDate", multi = false, "primitive", Some(e.text), None, "citation")
        case e @ Elem("ddm", "accessRights", _, _, _) => addPrimitiveFieldToMetadataBlock("accessrights", multi = false, "controlledVocabulary", Some(e.text), None, "access_and_licence")
        //TEST MET DUMMY WAARDE VOOR DEPOSIT AGREEMENT
        case e @ Elem("ddm", "available", _, _, _) => addPrimitiveFieldToMetadataBlock("accept", multi = false, "controlledVocabulary", Some(e.text), None, "depositAgreement")
        case _ => ()
      }
    }

    def addPrimitiveFieldToMetadataBlock(fieldName: String, multi: Boolean, typeClass: String, value: Option[String], values: Option[List[String]], metadataBlockName: String): Unit = {
      if (!multi)
        getMetadatablockFieldList(metadataBlockName) += PrimitiveFieldSingleValue(fieldName, multiple = multi, typeClass, value.getOrElse(""))
      else
        getMetadatablockFieldList(metadataBlockName) += PrimitiveFieldMultipleValues(fieldName, multiple = multi, typeClass, values.getOrElse(List()))
    }

    def mapPrimitiveFieldsMultiple(node: Node): Unit = {
      val audience = (node \\ "audience").map(_.text).toList
      addPrimitiveFieldToMetadataBlock("subject", multi = true, "controlledVocabulary", None, Some(audience), "citation")
    }

    def getMetadatablockFieldList(name: String): ListBuffer[Field] = {
      name match {
        case "citation" => citationfields
        case "access_and_licence" => access_and_LicenceFields
        case "depositAgreement" => depositAgreementFields
        case _ => new ListBuffer[Field]
      }
    }

    def getAuthorName(node: Node): String = {
      val author = node \ "author"
      var authorName = new ListBuffer[String]
      author.head.child.foreach {
        case e @ Elem(str, "titles", data, binding, node) => authorName += e.text
        case e @ Elem(str, "initials", data, binding, node) => authorName += e.text
        case e @ Elem(str, "surname", data, binding, node) => authorName += e.text
        case _ => ()
      }
      authorName.mkString(",")
    }

    def addCreator(cf: Node): Unit = {
      val author = cf \ "author"
      println("AUTHOR: " + author.head)
      var subFields = collection.mutable.Map[String, Field]()
      subFields += ("authorName" -> PrimitiveFieldSingleValue("authorName", multiple = false, "primitive", getAuthorName(cf)))

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
      citationfields += CompoundField("author", multiple = true, "compound", List(subFields))
    }

    def addContributor(cf: Node): Unit = {
      ???
    }

    val citationBlock = MetadataBlock("Citation Metadata", citationfields.toList)
    val access_and_licenseBlock = MetadataBlock("Access and License", access_and_LicenceFields.toList)
    val depositAgreementBlock = MetadataBlock("Deposit Agreement", depositAgreementFields.toList)
    val datasetVersion = DatasetVersion(Map("citation" -> citationBlock, "depositAgreement" -> depositAgreementBlock, "access-and-license" -> access_and_licenseBlock))
    val dataverseDataset = DataverseDataset(datasetVersion)

    println(Serialization.writePretty(dataverseDataset))

    dataverse.dataverse("root").createDataset(Serialization.writePretty(dataverseDataset)).map(_ => ())
  }
}






