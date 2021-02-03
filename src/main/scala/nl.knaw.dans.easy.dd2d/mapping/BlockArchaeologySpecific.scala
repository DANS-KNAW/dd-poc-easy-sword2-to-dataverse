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

trait BlockArchaeologySpecific {
  val ARCHIS_ZAAK_ID = "dansArchisZaakId"

  val ABR_RAPPORT_TYPE = "dansAbrRapportType"
  val ABR_RAPPORT_TYPE_VOCABULARY = "dansAbrRapportTypeVocabulary"
  val ABR_RAPPORT_TYPE_VOCABULARY_URI = "dansAbrRapportTypeVocabularyURI"
  val ABR_RAPPORT_TYPE_TERM = "dansAbrRapportTypeTerm"
  val ABR_RAPPORT_TYPE_TERM_URI = "dansAbrRapportTypeTermURI"

  val ABR_RAPPORT_NUMMER = "dansAbrRapportNummer"

  val ABR_VERWERVINGSWIJZE = "dansAbrVerwervingswijze"
  val ABR_VERWERVINGSWIJZE_VOCABULARY = "dansAbrVerwervingswijzeVocabulary"
  val ABR_VERWERVINGSWIJZE_VOCABULARY_URI = "dansAbrVerwervingswijzeVocabularyURI"
  val ABR_VERWERVINGSWIJZE_TERM = "dansAbrVerwervingswijzeTerm"
  val ABR_VERWERVINGSWIJZE_TERM_URI = "dansAbrVerwervingswijzeTermURI"

  val ABR_COMPLEX = "dansAbrComplex"
  val ABR_COMPLEX_VOCABULARY = "dansAbrComplexVocabulary"
  val ABR_COMPLEX_VOCABULARY_URI = "dansAbrComplexVocabularyURI"
  val ABR_COMPLEX_TERM = "dansAbrComplexTerm"
  val ABR_COMPLEX_TERM_URI = "dansAbrComplexTermURI"

  val ABR_PERIOD = "dansAbrPeriod"
  val ABR_PERIOD_VOCABULARY = "dansAbrPeriodVocabulary"
  val ABR_PERIOD_VOCABULARY_URI = "dansAbrPeriodVocabularyURI"
  val ABR_PERIOD_TERM = "dansAbrPeriodTerm"
  val ABR_PERIOD_TERM_URI = "dansAbrPeriodTermURI"
}
