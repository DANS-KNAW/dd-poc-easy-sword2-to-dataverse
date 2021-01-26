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

import nl.knaw.dans.lib.dataverse.DataverseInstance
import nl.knaw.dans.lib.error.TraversableTryExtensions
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.util.Try

class WorkflowDisabler(instance: DataverseInstance) extends DebugEnhancedLogging {
  type DisabledWorkflowsMemo = Map[String, Int]

  def disableDefaultWorkflows(triggerTypesToDisable: List[String]): Try[DisabledWorkflowsMemo] = {
    trace(triggerTypesToDisable)
    for {
      triggerTypeToId <- triggerTypesToDisable.map(getTriggerTypeToWorkflowId).collectResults.map(_.filter(_.isDefined).map(_.get).toMap)
      _ = triggerTypeToId.keys.map(unsetDefaultWorkflow)
    } yield triggerTypeToId
  }

  private def getTriggerTypeToWorkflowId(triggerType: String): Try[Option[(String, Int)]] = {
    getDefaultWorkflow(triggerType).map(optId => optId.map(id => (triggerType, id)))
  }

  private def getDefaultWorkflow(triggerType: String): Try[Option[Int]] = {
    for {
      r <- instance.admin().getDefaultWorkflow(triggerType)
      w <- r.data
    } yield w.id
  }

  private def unsetDefaultWorkflow(triggerType: String): Try[Unit] = {
    instance.admin().unsetDefaultWorkflow(triggerType).map(_ => logger.info(s"Disabled default workflow for trigger type: $triggerType"))
  }

  def restoreDefaultWorkflows(disabledWorkflowsMemo: DisabledWorkflowsMemo): Try[Unit] = {
    disabledWorkflowsMemo.map {
      case (k, v) => for {
        _ <- instance.admin().setDefaultWorkflow(k, v)
        _ = logger.info(s"Restored default workflow for trigger type: ${k}")
      } yield ()
    }.collectResults.map(_ => ())
  }
}
