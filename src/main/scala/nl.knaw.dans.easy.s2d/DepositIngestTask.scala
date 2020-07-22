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

  override def run(): Try[Unit] = {
    trace(())
    debug(s"Ingesting $deposit into Dataverse")

    // TODO: validate: is this a deposit can does it contain a bag that conforms to DANS BagIt Profile? (call easy-validate-dans-bag)
    implicit val format = DefaultFormats
    var metadataBlockFields = new ListBuffer[Field]


    def addPrimitiveFieldToMetadataBlock(fieldName: String, multi: Boolean, typeClass: String, value: Option[String], values: Option[List[String]]): Unit = {
      if (!multi)
        metadataBlockFields += PrimitiveFieldSingleValue(fieldName, multiple = multi, typeClass, value.getOrElse(""))
      else
        metadataBlockFields += PrimitiveFieldMultipleValues(fieldName, multiple = multi, typeClass, values.getOrElse(List()))
    }

    //EASY XML
    val ddm = XML.loadFile("/Users/chrismuller/Development/Git/Github/DANS-KNAW/dd-easy-sword2-to-dataverse/data/inbox/valid-easy-submitted/example-bag-medium/metadata/dataset.xml")
    //val ddm = deposit.tryDdm.getOrElse(Failure).asInstanceOf[Node]
    val profile = ddm \ "profile"
    val dcmiMetadata = ddm \ "dcmiMetadata"
    //join profile and dcmi parts for iteration
    val joined = profile ++ dcmiMetadata


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
      metadataBlockFields += CompoundField("author", multiple = true, "compound", List(subFields))
    }

    def addContributor(cf: Node): Unit = {
      ???
    }

    def mapPrimitiveFieldsMultiple(node: Node): Unit = {
      val audience = (node \\ "audience").map(_.text).toList
      addPrimitiveFieldToMetadataBlock("subject", multi = true, "controlledVocabulary", None, Some(audience))
    }

    def mapFields(node: Node): Unit = {
      val title = (node \\ "title").head.text
      addPrimitiveFieldToMetadataBlock("title", false, "primitive", Some(title), None)
      mapPrimitiveFieldsMultiple(node)
      node.child.foreach {
        case e @ Elem("dcterms", "alternative", _, _, _) => addPrimitiveFieldToMetadataBlock("alternativeTitle", multi = false, "primitive", Some(e.text), None)
        case node @ _ if node.label.equals("creatorDetails") => addCreator(node)
        case e @ Elem("ddm", "created", _, _, _) => addPrimitiveFieldToMetadataBlock("productionDate", multi = false, "primitive", Some(e.text), None)
        case _ => ()
      }
    }


    mapFields(joined.head)
    mapPrimitiveFieldsMultiple(joined.head)


    val metadataBlock = MetadataBlock("Citation Metadata", metadataBlockFields.toList)
    val datasetVersion = DatasetVersion(Map("citation" -> metadataBlock))
    val dataverseDataset = DataverseDataset(datasetVersion)

    println(Serialization.writePretty(dataverseDataset))


    dataverse.dataverse("root").createDataset(Serialization.writePretty(dataverseDataset)).map(_ => ())
  }
}






