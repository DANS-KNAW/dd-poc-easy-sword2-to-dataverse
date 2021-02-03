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

trait AbrScheme {
  val ABR_COMPLEX_SCHEME = "Archeologisch Basis Register" // ABR Complextypen
  val ABR_COMPLEX_SCHEME_URI = "http://www.rnaproject.org" // https://data.cultureelerfgoed.nl/term/id/abr/e9546020-4b28-4819-b0c2-29e7c864c5c0

  val ABR_PERIOD_SCHEME = "Archeologisch Basis Register" // ABR Periodes
  val ABR_PERIOD_SCHEME_URI = "http://www.rnaproject.org" // https://data.cultureelerfgoed.nl/term/id/abr/9b688754-1315-484b-9c89-8817e87c1e84

  val ABR_RAPPORT_TYPE_SCHEME = "ABR Rapporten"
  val ABR_RAPPORT_TYPE_SCHEME_URI = "https://data.cultureelerfgoed.nl/term/id/abr/7a99aaba-c1e7-49a4-9dd8-d295dbcc870e"

  val ABR_VERWERVINGSWIJZE_SCHEME = "ABR verwervingswijzen"
  val ABR_VERWERVINGSWIJZE_SCHEME_URI = "https://data.cultureelerfgoed.nl/term/id/abr/554ca1ec-3ed8-42d3-ae4b-47bcb848b238"
}
