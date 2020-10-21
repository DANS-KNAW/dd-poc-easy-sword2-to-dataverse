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

trait BlockCitation {
  val TITLE = "title"
  val SUBTITLE = "subtitle"
  val ALTERNATIVE_TITLE = "alternativeTitle"
  val ALTERNATIVE_URL = "alternativeURL"
  val OTHER_ID = "otherId"
  val OTHER_ID_AGENCY = "otherIdAgency"
  val OTHER_ID_VALUE = "otherIdValue"
  val AUTHOR = "author"
  val AUTHOR_NAME = "authorName"
  val AUTHOR_AFFILIATION = "authorAffiliation"
  val AUTHOR_IDENTIFIER_SCHEME = "authorIdentifierScheme"
  val AUTHOR_IDENTIFIER = "authorIdentifier"
  val DATASET_CONTACT = "datasetContact"
  val DATASET_CONTACT_NAME = "datasetContactName"
  val DATASET_CONTACT_AFFILIATION = "datasetContactAffiliation"
  val DATASET_CONTACT_EMAIL = "datasetContactEmail"
  val DESCRIPTION = "dsDescription"
  val DESCRIPTION_VALUE = "dsDescriptionValue"
  val DESCRIPTION_DATE = "dsDescriptionDate"
  val SUBJECT = "subject"
  val KEYWORD = "keyword"
  val KEYWORD_VALUE = "keywordValue"
  val KEYWORD_VOCABULARY = "keywordVocabulary"
  val KEYWORD_VOCABULARY_URI = "keywordVocabularyURI"
  val TOPIC_CLASSIFICATION = "topicClassification"
  val TOPIC_CLASSVALUE = "topicClassValue"
  val TOPIC_CLASSVOCAB = "topicClassVocab"
  val TOPIC_CLASSVOCAB_URI = "topicClassVocabURI"
  val PUBLICATION = "publication"
  val PUBLICATION_CITATION = "publicationCitation"
  val PUBLICATION_ID_TYPE = "publicationIDType"
  val PUBLICATION_ID_NUMBER = "publicationIDNumber"
  val PUBLICATION_URL = "publicationURL"
  val NOTES_TEXT = "notesText"
  val LANGUAGE = "language"
  val PRODUCER = "producer"
  val PRODUCER_NAME = "producerName"
  val PRODUCER_AFFILIATION = "producerAffiliation"
  val PRODUCER_ABBREVIATION = "producerAbbreviation"
  val PRODUCER_URL = "producerURL"
  val PRODUCER_LOGO_URL = "producerLogoURL"
  val PRODUCTION_DATE = "productionDate"
  val PRODUCTION_PLACE = "productionPlace"
  val CONTRIBUTOR = "contributor"
  val CONTRIBUTOR_TYPE = "contributorType"
  val CONTRIBUTOR_NAME = "contributorName"
  val GRANT_NUMBER = "grantNumber"
  val GRANT_NUMBER_AGENCY = "grantNumberAgency"
  val GRANT_NUMBER_VALUE = "grantNumberValue"
  val DISTRIBUTOR = "distributor"
  val DISTRIBUTOR_NAME = "distributorName"
  val DISTRIBUTOR_AFFILIATION = "distributorAffiliation"
  val DISTRIBUTOR_ABBREVIATION = "distributorAbbreviation"
  val DISTRIBUTOR_URL = "distributorURL"
  val DISTRIBUTOR_LOGO_URL = "distributorLogoURL"
  val DISTRIBUTION_DATE = "distributionDate"
  val DEPOSITOR = "depositor"
  val DATE_OF_DEPOSIT = "dateOfDeposit"
  val TIME_PERIOD_COVERED = "timePeriodCovered"
  val TIME_PERIOD_COVERED_START = "timePeriodCoveredStart"
  val TIME_PERIOD_COVERED_END = "timePeriodCoveredEnd"
  val DATE_OF_COLLECTION = "dateOfCollection"
  val DATE_OF_COLLECTION_START = "dateOfCollectionStart"
  val DATE_OF_COLLECTION_END = "dateOfCollectionEnd"
  val KIND_OF_DATA = "kindOfData"
  val SERIES = "series"
  val SERIES_NAME = "seriesName"
  val SERIES_INFORMATION = "seriesInformation"
  val SOFTWARE = "software"
  val SOFTWARE_NAME = "softwareName"
  val SOFTWARE_VERSION = "softwareVersion"
  val RELATED_MATERIAL = "relatedMaterial"
  val RELATED_DATASETS = "relatedDatasets"
  val OTHER_REFERENCES = "otherReferences"
  val DATA_SOURCES = "dataSources"
  val ORIGIN_OF_SOURCES = "originOfSources"
  val CHARACTERISTICS_OF_SOURCES = "characteristicOfSources"
  val ACCESS_TO_SOURCES = "accessToSources"
}
