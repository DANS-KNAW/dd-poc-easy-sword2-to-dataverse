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
import nl.knaw.dans.lib.error._
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.language.reflectiveCalls
import scala.util.Try
import scala.util.control.NonFatal

object Command extends App with DebugEnhancedLogging {
  type FeedBackMessage = String

  val configuration = Configuration(File(System.getProperty("app.home")))
  val commandLine: CommandLineOptions = new CommandLineOptions(args, configuration) {
    verify()
  }

  val app = new DansDeposit2ToDataverseApp(configuration)
  val result = for {
    _ <- app.checkPreconditions()
    msg <- commandLine.subcommand match {
      case Some(cmd @ commandLine.importCommand) => {
        if (cmd.singleDeposit()) app.importSingleDeposit(cmd.depositsInboxOrSingleDeposit(), !cmd.draft())
        else app.importDeposits(cmd.depositsInboxOrSingleDeposit(), !cmd.draft())
      }.map(_ => "Done importing deposits")
      case Some(_ @ commandLine.runService) => runAsService()
      case _ => Try { s"Unknown command: ${ commandLine.subcommand }" }
    }
  } yield msg

  result.doIfSuccess(msg => Console.err.println(s"OK: $msg"))
    .doIfFailure { case e => logger.error(e.getMessage, e) }
    .doIfFailure { case NonFatal(e) => Console.err.println(s"FAILED: ${ e.getMessage }") }

  private def runAsService(): Try[FeedBackMessage] = Try {
    Runtime.getRuntime.addShutdownHook(new Thread("service-shutdown") {
      override def run(): Unit = {
        logger.info("Received request to shut down service ...")
        app.stop()
        logger.info("Service stopped.")
      }
    })
    app.start()
    logger.info("Service started ...")
    Thread.currentThread.join()
    "Service terminated normally."
  }
}
