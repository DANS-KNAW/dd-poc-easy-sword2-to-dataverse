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

import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.xml.Node

/**
 * ddm:audience element with a NARCIS classification code in it.
 * Used for Subject field in the Citation metadata block
 */
object Audience extends DebugEnhancedLogging {
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
    "D41" -> "Law",
    "D5" -> "Social Sciences",
    "D6" -> "Social Sciences",
    "D42" -> "Social Sciences",
    "D7" -> "Business and Management",
    "D15" -> "Earth and Environmental Sciences",
    "E15" -> "Earth and Environmental Sciences",
  )

  /**
   * Returns the best match for this NARCIS classification code in the Dataverse subject vocabulary
   * used in the Citation metadata block
   *
   * @param node the audience element
   * @return the Dataverse subject term
   */
  def toCitationBlockSubject(node: Node): Option[String] = {
    if (!node.text.matches("""^[D|E]\d{5}$""")) {
      throw new RuntimeException("NARCIS classification code incorrectly formatted")
    }

    Some(narcisToSubject
      .toSeq
      .filter { case (x, _) => node.text.startsWith(x) })
      .collect {
        case l if l.nonEmpty => l.maxBy(_._1.length)._2
        case _ => "Other"
      }
  }

  def toNarcisTerm(node: Node): Option[String] = {
      Option("https://www.narcis.nl/classification/" + node.text)
  }
}
