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

trait Contributor {
  val contributoreRoleToContributorType = Map(
    "DataCurator" -> "Data Curator",
    "DataManager" -> "Data Manager",
    "Editor" -> "Editor",
    "Funder" -> "Funder", // Is not actually present in the DataCite vocabulary, so it should never be found in the input
    "HostingInstitution" -> "Hosting Institution",
    "ProjectLeader" -> "Project Leader",
    "ProjectManager" -> "Project Manager",
    "Related Person" -> "Related Person",
    "Researcher" -> "Researcher",
    "ResearchGroup" -> "Research Group",
    "RightsHolder" -> "Rights Holder",
    "Sponsor" -> "Sponsor",
    "Supervisor" -> "Supervisor",
    "WorkPackageLeader" -> "Work Package Leader",

    // Other, or not available in the target vocabulary
    "Other" -> "Other",
    "Producer" -> "Other",
    "RegistrationAuthority" -> "Other",
    "RegistrationAgency" -> "Other",
    "Distributor" -> "Other",
    "DataCollector" -> "Other",
    "ContactPerson" -> "Other")
}
