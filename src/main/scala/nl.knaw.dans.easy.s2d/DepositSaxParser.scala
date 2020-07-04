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

import java.io.FileInputStream
import java.util

import javax.xml.parsers.SAXParserFactory
import nl.knaw.dans.easy.s2d.dataverse.json.{ CompoundField, Field, MetadataBlock, PrimitiveField }
import org.json4s.native.Serialization
import org.json4s.{ DefaultFormats, Formats }
import org.xml.sax.helpers.DefaultHandler
import org.xml.sax.{ Attributes, SAXException }

import scala.collection.mutable.ListBuffer

class DepositSaxParser extends DefaultHandler {
  implicit val jsonFormats: Formats = DefaultFormats

  //flags
  var isAuthor = false

  //value of element
  var elementValue: String = ""

  var fields = new ListBuffer[Field]
  //Deqeue for storing all fields to feed to Metadatablock
  var metadataBlockFields = new util.ArrayDeque[Field](30)

  //Deque for storing fields in compound field
  var compoundFieldArray = new ListBuffer[Field]


  def parseDocument (xml: FileInputStream) : String = {
    val factory = SAXParserFactory.newInstance()
    val parser = factory.newSAXParser()
    parser.parse(xml, this)
    Serialization.writePretty(MetadataBlock("citation", fields.toList))
  }


  @throws[SAXException]
  override def characters(ch: Array[Char], start: Int, length: Int): Unit = {
    elementValue = new String(ch, start, length)
  }

  override def startElement(uri: String, localName: String, qName: String, attributes: Attributes): Unit = {

    //set compound flags
    //add nested primitive files to Deqeue

    if (qName.contains("author")) {
      isAuthor = true
    }
    if (isAuthor && qName.contains("initials")) {
      val initials = PrimitiveField(localName, multiple = false, "primitive", qName)
      compoundFieldArray += (initials)
    }

    if (isAuthor && qName.contains("surname")) {
      val surname = PrimitiveField(localName, multiple = false, "primitive", qName)
      compoundFieldArray += (surname)
    }
  }

  override def endElement(uri: DepositName, localName: DepositName, qName: DepositName): Unit = {

    //add non nested primitive fields and compound fields

    if (qName.contains("title")) {
      metadataBlockFields.add(PrimitiveField("title", multiple = false, "primitive", qName))
    }

    if (qName.contains("author")) {
      isAuthor = false
      metadataBlockFields.add(CompoundField("author", multiple = true, "compound", compoundFieldArray.toList))
    }
  }

  override def endDocument(): Unit = {
    //deqeue is to maintain insertion order
    while (!metadataBlockFields.isEmpty) {
      val field = metadataBlockFields.poll()
      fields.+=(field)
    }
  }
}
