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

trait BlockContentTypeAndFileFormat {
  val CONTENT_TYPE = "dansContentType"
  val CONTENT_TYPE_FREE = "dansContentTypeFree"
  val FORMAT = "dansFormat"
  val FORMAT_FREE = "dansFormatFree"
  val CONTAINS_CLARIN_METADATA = "dansContainsClarinMetadata"
  val CONTENT_TYPE_CV = "dansContentTypeCv"
  val CONTENT_TYPE_CV_VALUE = "dansContentTypeCvValue"
  val CONTENT_TYPE_CV_VOCABULARY = "dansContentTypeCvVocabulary"
  val CONTENT_TYPE_CV_VOCABULARY_URL = "dansContentTypeCvVocabularyURI"
  val FORMAT_CV = "dansFormatCv"
  val FORMAT_CV_VALUE = "dansFormatCvValue"
  val FORMAT_CV_VOCABULARY = "dansFormatCvVocabulary"
  val FORMAT_CV_VOCABUALRY_URL = "dansFormatCvVocabularyURI"
  val DCMI_TYPE = "DCMI Type Vocabulary"
  val DCMI_FORMAT = "MediaType"
  val DCMI_TYPE_BASE_URL = "https://www.dublincore.org/specifications/dublin-core/dcmi-terms/dcmitype/"
  val DCMI_FORMAT_BASE_URL = "https://www.iana.org/assignments/media-types/"
}

