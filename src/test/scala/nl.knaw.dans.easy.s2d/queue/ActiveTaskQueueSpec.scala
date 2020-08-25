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
package nl.knaw.dans.easy.s2d.queue

import org.scalatest.{ FlatSpec, Matchers, OneInstancePerTest }

class ActiveTaskQueueSpec extends FlatSpec with Matchers with OneInstancePerTest {

  // TODO: how to make this test robust?
  "start" should "cause previously queued items to be processed (within a reasonable time)" in {
    val q = ActiveTaskQueue()
    val triggeredTasks = List(
      TriggerTask(),
      TriggerTask(),
      TriggerTask()
    )
    val nonTriggerdTasks = List(
      TriggerTask(),
      TriggerTask(),
      TriggerTask()
    )
    triggeredTasks.foreach(q.add)
    q.start()
    Thread.sleep(5000)
    q.stop()
    triggeredTasks(0).triggered shouldBe true
    triggeredTasks(1).triggered shouldBe true
    triggeredTasks(2).triggered shouldBe true
  }
}
