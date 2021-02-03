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

import better.files.File
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{ Inside, OneInstancePerTest }

trait TestSupportFixture extends AnyFlatSpec with Matchers with Inside with CustomMatchers with OneInstancePerTest with JsonPathSupportFixture {

  lazy val testDirValid: File = File("src/test/resources/examples")
  lazy val testDirNonValid: File = File("src/test/resources/no-deposit")
  lazy val testDirUnorderedDeposits = File("src/test/resources/unordered-stub-deposits")
}

