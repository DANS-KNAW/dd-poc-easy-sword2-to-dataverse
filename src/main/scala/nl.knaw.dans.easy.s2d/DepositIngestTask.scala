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
    //Deqeue for storing all fields to feed to Metadatablock
    var metadataBlockFields = new ListBuffer[Field]

    def addPrimitiveFieldToMetadataBlock(fieldName: String, multi: Boolean, typeClass: String, value: Option[String], values: Option[List[String]]): Unit = {
      if (!multi)
        metadataBlockFields += PrimitiveFieldSingleValue(fieldName, multiple = multi, "primitive", value.getOrElse(""))
      else
        metadataBlockFields += PrimitiveFieldMultipleValues(fieldName, multiple = multi, "primitive", values.getOrElse(List()))
    }

    //welke titel nemen we mee?
    def addTitle(node: Node): Unit = {
      val title = (node \\ "title").head.text
      addPrimitiveFieldToMetadataBlock("title", false, "primitive", Some(title), None)
    }

    //EASY XML
    val xmlNode = XML.loadFile("/Users/chrismuller/Development/Git/Github/DANS-KNAW/dd-easy-sword2-to-dataverse/data/inbox/valid-easy-submitted/example-bag-medium/metadata/dataset1.xml")
    val profile = xmlNode \ "profile"
    val dcmiMetadata = xmlNode \ "dcmiMetadata"
    //join profile and dcmi parts for iteration
    val joined = profile ++ dcmiMetadata
    //val joined = xmlNode.map(x => x ++ x)

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
        case node @ _ if node.label.equals("organization") => subFields +=  ("authorAffiliation" -> PrimitiveFieldSingleValue("authorAffiliation", false, "primitive", (node \ "name").head.text))
        case e @ Elem("dcx-dai", "DAI", _, _, _) => {
          subFields += ("authorIdentifierScheme" -> PrimitiveFieldSingleValue("authorIdentifierScheme", multiple = false, "controlledVocabulary", e.label))
          subFields += ("authorIdentifier" ->  PrimitiveFieldSingleValue("authorIdentifier", multiple = false, "primitive", e.text))
        }
        case e @ Elem("dcx-dai", "ISNI", _, _, _) => {
          subFields += ("authorIdentifierScheme" -> PrimitiveFieldSingleValue("authorIdentifierScheme", multiple = false, "controlledVocabulary", e.label))
          subFields += ("authorIdentifier" ->  PrimitiveFieldSingleValue("authorIdentifier", multiple = false, "primitive", e.text))
        }
        case e @ Elem("dcx-dai", "ORCID", _, _, _) => {
          subFields +=  ("authorIdentifierScheme" -> PrimitiveFieldSingleValue("authorIdentifierScheme", multiple = false, "controlledVocabulary", e.label))
          subFields += ("authorIdentifier" ->  PrimitiveFieldSingleValue("authorIdentifier", multiple = false, "primitive", e.text))
        }
        case _ => ()
      }
      metadataBlockFields += CompoundField("author", multiple = true, "compound", List(subFields))
    }

    def addContributor(cf: Node): Unit = {
      ???
    }

    def addAudience(node: Node): Unit = {
      val values = (node \\ "audience").map(_.text).toList
      addPrimitiveFieldToMetadataBlock("subject", multi = true, "controlledVocabulary", None, Some(values))
    }

    addTitle(joined.head)
//    addAudience(joined.head)
    joined.head.child.foreach {
      case e @ Elem("dcterms", "alternative", _, _, _) => addPrimitiveFieldToMetadataBlock("alternativeTitle", false, "primitive", Some(e.text), None)
    //  case e @ Elem(_, "description", _, _, _) => addPrimitiveFieldToMetadataBlock("description", false, "primitive", Some(e.text), None)
      case node @ _ if node.label.equals("creatorDetails") => addCreator(node)
    //  case node @ _ if node.label.equals("contributorDetails") => addCreator(node)
      //  case e @ Elem(_, "hasVersion", _, _, _) => addPrimitiveFieldToMetadataBlock(e, false, )
      //case e @ Elem("ddm", "audience", _, _, _) => addPrimitiveFieldToMetadataBlock("subject", true, "controlledVocabulary", e)
      case e @ Elem("ddm", "created", _, _, _) => addPrimitiveFieldToMetadataBlock("productionDate", false, "primitive", Some(e.text), None)
      case _ => ()
    }

    val metadataBlock = MetadataBlock("Citation Metadata", metadataBlockFields.toList)
    val datasetVersion = DatasetVersion(Map("citation" -> metadataBlock))
    val dataverseDataset = DataverseDataset(datasetVersion)

    val result = Serialization.writePretty(dataverseDataset)
    println(result)

    //val res = "{\n\t\"datasetVersion\": {\n\t\t\"metadataBlocks\": {\n\t\t\t\"citation\": {\n\t\t\t\t\"displayName\": \"citation\",\n\t\t\t\t\"fields\": [{\n\t\t\t\t\t\t\"typeName\": \"title\",\n\t\t\t\t\t\t\"multiple\": false,\n\t\t\t\t\t\t\"typeClass\": \"primitive\",\n\t\t\t\t\t\t\"value\": \"Title of the dataset 6\"\n\t\t\t\t\t},\n\t\t\t\t\t{\n\t\t\t\t\t\t\"typeName\": \"author\",\n\t\t\t\t\t\t\"multiple\": true,\n\t\t\t\t\t\t\"typeClass\": \"compound\",\n\t\t\t\t\t\t\"value\": [{\n\t\t\t\t\t\t\t\t\"typeName\": \"authorName\",\n\t\t\t\t\t\t\t\t\"multiple\": false,\n\t\t\t\t\t\t\t\t\"typeClass\": \"primitive\",\n\t\t\t\t\t\t\t\t\"value\": \"Prof.,D.N.,Aarden\"\n\t\t\t\t\t\t\t},\n\t\t\t\t\t\t\t{\n\t\t\t\t\t\t\t\t\"typeName\": \"authorIdentifierScheme\",\n\t\t\t\t\t\t\t\t\"multiple\": false,\n\t\t\t\t\t\t\t\t\"typeClass\": \"controlledVocabulary\",\n\t\t\t\t\t\t\t\t\"value\": \"DAI\"\n\t\t\t\t\t\t\t},\n\t\t\t\t\t\t\t{\n\t\t\t\t\t\t\t\t\"typeName\": \"authorAffiliation\",\n\t\t\t\t\t\t\t\t\"multiple\": false,\n\t\t\t\t\t\t\t\t\"typeClass\": \"primitive\",\n\t\t\t\t\t\t\t\t\"value\": \"Utrecht University\"\n\t\t\t\t\t\t\t},\n\t\t\t\t\t\t\t{\n\t\t\t\t\t\t\t\t\"typeName\": \"authorIdentifier\",\n\t\t\t\t\t\t\t\t\"multiple\": false,\n\t\t\t\t\t\t\t\t\"typeClass\": \"primitive\",\n\t\t\t\t\t\t\t\t\"value\": \"123456789\"\n\t\t\t\t\t\t\t}\n\n\t\t\t\t\t\t]\n\t\t\t\t\t}\n\t\t\t\t]\n\t\t\t}\n\t\t}\n\t}\n}"



    dataverse.dataverse("root").createDataset(Serialization.writePretty(dataverseDataset)).map(_ => ())
  }
}








