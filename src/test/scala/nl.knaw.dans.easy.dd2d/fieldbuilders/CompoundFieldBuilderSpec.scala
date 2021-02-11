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
package nl.knaw.dans.easy.dd2d.fieldbuilders

import nl.knaw.dans.easy.dd2d.JsonPathSupportFixture
import nl.knaw.dans.easy.dd2d.mapping.FieldMap
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.json4s.{ DefaultFormats, Extraction, Formats, JBool, JString }
import org.json4s.native.Serialization
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CompoundFieldBuilderSpec extends AnyFlatSpec with Matchers with JsonPathSupportFixture with DebugEnhancedLogging {
  private implicit val jsonFormats: Formats = DefaultFormats

  "build" should "return None if no values are present" in {
    new CompoundFieldBuilder("test").build() shouldBe None
  }

  it should "return a complex field with one value if one value is provided" in {
    val c = new CompoundFieldBuilder("test")
    val value = FieldMap()
    value.addPrimitiveField("subfield", "subfield value")
    c.addValue(value.toJsonObject)
    val optField = c.build()
    optField.isDefined shouldBe true
    val field = optField.get
    val json = Serialization.writePretty(field)
    debug(json)
    getPathAsString(json, "$.typeClass") shouldBe "compound"
    getPathAsString(json, "$.typeName") shouldBe "test"
    getPathAsBoolean(json, "$.multiple") shouldBe true
    getPathAsString(json, "$.value[0].subfield.typeClass") shouldBe "primitive"
    getPathAsString(json, "$.value[0].subfield.typeName") shouldBe "subfield"
    getPathAsBoolean(json, "$.value[0].subfield.multiple") shouldBe false
    getPathAsString(json, "$.value[0].subfield.value") shouldBe "subfield value"
  }

  it should "return a single-value field if so specified" in {
    val c = new CompoundFieldBuilder("test", multipleValues = false)
    val value = FieldMap()
    value.addPrimitiveField("subfield", "subfield value")
    c.addValue(value.toJsonObject)
    val optField = c.build()
    optField.isDefined shouldBe true
    val field = optField.get
    val json = Serialization.writePretty(field)
    debug(json)
    getPathAsBoolean(json, "$.multiple") shouldBe false
  }

  it should "throw an exception when a second value is added to a single value list" in {
    val c = new CompoundFieldBuilder("test", multipleValues = false)
    val value1 = FieldMap()
    value1.addPrimitiveField("subfield", "value 1 ")
    val value2 = FieldMap()
    value2.addPrimitiveField("subfield", "value 2")
    c.addValue(value1.toJsonObject)
    an [IllegalArgumentException] should be thrownBy {
      c.addValue(value2.toJsonObject)
    }
  }

  it should "output two fields if two were added" in {
    val c = new CompoundFieldBuilder("test", multipleValues = true)
    val value1 = FieldMap()
    value1.addPrimitiveField("subfield", "value 1")
    val value2 = FieldMap()
    value2.addPrimitiveField("subfield", "value 2")
    c.addValue(value1.toJsonObject)
    c.addValue(value2.toJsonObject)
    val optField = c.build()
    optField.isDefined shouldBe true
    val field = optField.get
    val json = Serialization.writePretty(field)
    debug(json)

    getPathAsString(json, "$.typeClass") shouldBe "compound"
    getPathAsString(json, "$.typeName") shouldBe "test"
    getPathAsBoolean(json, "$.multiple") shouldBe true
    getPathAsString(json, "$.value[0].subfield.typeClass") shouldBe "primitive"
    getPathAsString(json, "$.value[0].subfield.typeName") shouldBe "subfield"
    getPathAsBoolean(json, "$.value[0].subfield.multiple") shouldBe false
    getPathAsString(json, "$.value[0].subfield.value") shouldBe "value 1"

    getPathAsString(json, "$.value[1].subfield.typeClass") shouldBe "primitive"
    getPathAsString(json, "$.value[1].subfield.typeName") shouldBe "subfield"
    getPathAsBoolean(json, "$.value[1].subfield.multiple") shouldBe false
    getPathAsString(json, "$.value[1].subfield.value") shouldBe "value 2"
  }

  it should "contain values of more than one subfield" in {
    val c = new CompoundFieldBuilder("test", multipleValues = true)
    val value1 = FieldMap()
    value1.addPrimitiveField("subfieldA", "value 1 A")
    value1.addPrimitiveField("subfieldB", "value 1 B")
    val value2 = FieldMap()
    value2.addPrimitiveField("subfieldA", "value 2 A")
    value2.addPrimitiveField("subfieldB", "value 2 B")
    c.addValue(value1.toJsonObject)
    c.addValue(value2.toJsonObject)
    val optField = c.build()
    optField.isDefined shouldBe true
    val field = optField.get
    val json = Serialization.writePretty(field)
    debug(json)

    getPathAsString(json, "$.typeClass") shouldBe "compound"
    getPathAsString(json, "$.typeName") shouldBe "test"
    getPathAsBoolean(json, "$.multiple") shouldBe true

    getPathAsString(json, "$.value[0].subfieldA.typeClass") shouldBe "primitive"
    getPathAsString(json, "$.value[0].subfieldA.typeName") shouldBe "subfieldA"
    getPathAsBoolean(json, "$.value[0].subfieldA.multiple") shouldBe false
    getPathAsString(json, "$.value[0].subfieldA.value") shouldBe "value 1 A"

    getPathAsString(json, "$.value[0].subfieldB.typeClass") shouldBe "primitive"
    getPathAsString(json, "$.value[0].subfieldB.typeName") shouldBe "subfieldB"
    getPathAsBoolean(json, "$.value[0].subfieldB.multiple") shouldBe false
    getPathAsString(json, "$.value[0].subfieldB.value") shouldBe "value 1 B"


    getPathAsString(json, "$.value[1].subfieldA.typeClass") shouldBe "primitive"
    getPathAsString(json, "$.value[1].subfieldA.typeName") shouldBe "subfieldA"
    getPathAsBoolean(json, "$.value[1].subfieldA.multiple") shouldBe false
    getPathAsString(json, "$.value[1].subfieldA.value") shouldBe "value 2 A"

    getPathAsString(json, "$.value[1].subfieldB.typeClass") shouldBe "primitive"
    getPathAsString(json, "$.value[1].subfieldB.typeName") shouldBe "subfieldB"
    getPathAsBoolean(json, "$.value[1].subfieldB.multiple") shouldBe false
    getPathAsString(json, "$.value[1].subfieldB.value") shouldBe "value 2 B"
  }
}
