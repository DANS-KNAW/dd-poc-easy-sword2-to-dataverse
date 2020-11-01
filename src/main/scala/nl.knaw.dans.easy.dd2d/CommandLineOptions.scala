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

import java.nio.file.Path

import org.rogach.scallop.{ ScallopConf, ScallopOption, Subcommand }

class CommandLineOptions(args: Array[String], configuration: Configuration) extends ScallopConf(args) {
  appendDefaultToDescription = true
  editBuilder(_.setHelpWidth(110))
  printedName = "dd-dans-deposit-to-dataverse"
  version(configuration.version)
  private val SUBCOMMAND_SEPARATOR = "---\n"
  val description: String = s"""Imports DANS deposit directories into Dataverse datasets."""
  val synopsis: String =
    s"""
       |  $printedName run-service
       |  $printedName import [-d,--draft] <inbox>
       |  $printedName import [-d,--draft] -s <single-deposit>
       |  """.stripMargin

  version(s"$printedName v${ configuration.version }")
  banner(
    s"""
       |  $description
       |
       |Usage:
       |
       |$synopsis
       |
       |Options:
       |""".stripMargin)

  shortSubcommandsHelp(true)
  val runService = new Subcommand("run-service") {
    descr(
      "Starts DANS Deposit To Dataverse as a daemon that processes deposit directories as they appear in the configured inbox.")
    footer(SUBCOMMAND_SEPARATOR)
  }
  addSubcommand(runService)
  val importCommand = new Subcommand("import") {
    descr("Imports one ore more deposits. Does not monitor for new deposits to arrive, but instead terminates after importing the batch.")
    val singleDeposit: ScallopOption[Boolean] = opt(name = "single", descr = "Single deposit instead of a deposits inbox")
    val draft: ScallopOption[Boolean] = opt(name = "draft", descr = "Do not publish the resulting datasets, but keep them on DRAFT status")
    val depositsInboxOrSingleDeposit: ScallopOption[Path] = trailArg(name = "inbox-or-single-deposit",
      descr = "Directory containing as sub-directories the deposit dirs to be imported or a single deposit")
    validatePathExists(depositsInboxOrSingleDeposit)
    footer(SUBCOMMAND_SEPARATOR)
  }
  addSubcommand(importCommand)


  footer("")
}
