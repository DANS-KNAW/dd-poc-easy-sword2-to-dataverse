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
package nl.knaw.dans.easy.dd2d.mapping

import scala.xml.NodeSeq

case class AudienceNodes(nodes: NodeSeq) {
  def toCitationBlockSubjects: List[String] = {
    val narcisToSubject = Map(
      "D11" -> "Mathematical Sciences",
      "D12" -> "Physics",
      "D13" -> "Chemistry",
      "D14" -> "Engineering",
      "D16" -> "Computer and Information Science",
      "D17" -> "Astronomy and Astrophysics",
      "D18" -> "Agricultural Sciences",
      "D2" -> "Medicine, Health and Life Sciences",
      "D3" -> "Arts and Humanities",
      "D4" -> "Law",
      "D6" -> "Social Sciences",
      "D7" -> "Business and Management",
      "E15" -> "Earth and Environmental Sciences"
    )

    nodes.toList.map {
      a =>
        val code = a.text.take(3).takeWhile(_ != '0') // TODO: check this algorithm
        narcisToSubject.getOrElse(code, "Other")
    }
  }
}
