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
package nl.knaw.dans.easy.s2d.dataverse

import java.io.PrintStream

case class DataverseInstance(config: DataverseInstanceConfig)(implicit resultOutputStream: PrintStream) {
  def dataverse(dvId: String): Dataverse = {
    Dataverse(dvId: String, config)
  }

  def dataset(id: String, isPersistentId: Boolean): Dataset = {
    Dataset(id, isPersistentId, config)
  }

  def file(id: String, isPersistentId: Boolean): FileCommand = {
    FileCommand(id, isPersistentId, config)
  }
}
