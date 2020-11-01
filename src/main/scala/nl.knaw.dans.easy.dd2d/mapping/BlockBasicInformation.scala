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

trait BlockBasicInformation {
  val LANGUAGE_OF_METADATA = "dansLanguageOfMetadata"
  val PEOPLE_AND_ORGANIZATIONS = "dansPno"
  val PEOPLE_AND_ORGANIZATIONS_ROLE = "dansPnoRole"
  val PEOPLE_AND_ORGANIZATIONS_ORGANIZATION = "dansPnoOrganization"
  val PEOPLE_AND_ORGANIZATIONS_TITLES = "dansPnoTitles"
  val PEOPLE_AND_ORGANIZATIONS_INITIALS = "dansPnoInitials"
  val PEOPLE_AND_ORGANIZATIONS_PREFIX = "dansPnoPrefix"
  val PEOPLE_AND_ORGANIZATIONS_SURNAME = "dansPnoSurname"
  val PEOPLE_AND_ORGANIZATIONS_ORCID = "dansPnoOrcid"
  val PEOPLE_AND_ORGANIZATIONS_ISNI = "dansPnoIsni"
  val PEOPLE_AND_ORGANIZATIONS_DAI = "dansPnoDai"
  val RELATED_ID = "dansRelatedId"
  val RELATED_ID_RELATION_TYPE = "dansRelatedIdRelationType"
  val RELATED_ID_SCHEME = "dansRelatedIdScheme"
  val RELATED_ID_IDENTIFIER = "dansRelatedIdIdentifier"
  val RELATED_ID_URL = "dansRelatedIdUrl"
  val RELATED_ID_URL_RELATION_TYPE = "dansRelatedIdUrlRelationType"
  val RELATED_ID_URL_TITLE = "dansRelatedIdUrlTitle"
  val RELATED_ID_URL_URL = "dansRelatedIdUrlUrl"
  val LANGUAGE_OF_FILES = "dansLanguageOfFiles"
  val DATE = "dansDate"
  val DATE_TYPE = "dansDateType"
  val DATE_VALUE = "dansDateValue"
  val DATE_FREE_FORMAT = "dansDateFreeFormat"
  val DATE_FREE_FORMAT_TYPE = "dansDateFreeFormatType"
  val DATE_FREE_FORMAT_VALUE = "dansDateFreeFormatValue"
  val RIGHTSHOLDER = "dansRightsholder"
  val RIGHTSHOLDER_ORGANIZATION = "dansRightsholderOrganization"
  val RIGHTSHOLDER_TITLES = "dansRightsholderTitles"
  val RIGHTSHOLDER_INITIALS = "dansRightsholderInitials"
  val RIGHTSHOLDER_PREFIX = "dansRightsholderPrefix"
  val RIGHTSHOLDER_SURNAME = "dansRightsholderSurname"
  val RIGHTSHOLDER_ORCID = "dansRightsholderOrcid"
  val RIGHTSHOLDER_ISNI = "dansRightsholderIsni"
  val RIGHTSHOLDER_DAI = "dansRightsholderDai"
  val SUBJECT_CV = "subjectCv"
  val SUBJECT_CV_VALUE = "subjectCvValue"
  val SUBJECT_CV_VOCABULARY = "subjectCvVocabulary"
  val SUBJECT_CV_VOCABULARY_URI = "subjectCvVocabularyURI"
  val SUBJECT_NARCIS_CLASSIFICATION_URL = "https://www.narcis.nl/classification/"
  val SUBJECT_NARCIS_CLASSIFICATION = "NARCIS classification"
  val ROLE_CV = "dansPno"
  val ROLE_CV_VALUE = "roleCvValue"
  val ROLE_CV_VOCABULARY = "roleCvVocabulary"
  val ROLE_CV_VOCABULARY_URI = "roleCvVocabularyURI"
  val ROLE_CV_DATACITE_CLASSIFICATION = "https://support.datacite.org/docs/schema-40/"
  val LANGUAGE_CV_ISO_639_2_URL = "https://www.loc.gov/standards/iso639-2/php/code_list.php"
  val LANGUAGE_OF_FILES_CV = "languageOfFilesCv"
  val LANGUAGE_OF_FILES_CV_VALUE = "languageOfFilesCvValue"
  val LANGUAGE_OF_FILES_CV_VOCABULARY = "languageOfFilesCvVocabulary"
  val LANGUAGE_OF_FILES_CV_VOCABULART_URL = "languageOfFilesCvVocabularyURI"
  val LANGUAGE_OF_FILES_CV_VOCABULARY_NAME = "ISO-639-2"
}
