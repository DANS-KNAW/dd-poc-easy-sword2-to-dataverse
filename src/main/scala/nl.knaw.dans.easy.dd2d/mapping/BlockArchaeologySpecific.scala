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
  val ABR_SUBJECT = "dansAbrSubjectCv"
  val ABR_SUBJECT_VALUE = "dansAbrSubjectCvValue"
  val ABR_SUBJECT_VOCABULARY = "dansAbrSubjectCvVocabulary"
  val ABR_SUBJECT_VOCABULARY_URL = "dansAbrSubjectCvVocabularyURI"
  val ABR_PERIOD = "dansAbrPeriodCv"
  val ABR_PERIOD_VALUE = "dansAbrPeriodCvValue"
  val ABR_PERIOD_VOCABULARY = "dansAbrPeriodCvVocabulary"
  val ABR_PERIOD_VOCABULARY_URL = "dansAbrPeriodCvVocabularyURI"
  val ABR_BASE_URL = "https://data.cultureelerfgoed.nl/term/id/abr/"
}
