package nl.knaw.dans.easy.dd2d.fieldbuilders

import nl.knaw.dans.lib.dataverse.model.dataset.MetadataField

abstract class AbstractFieldBuilder {

  /**
   * Builds the field, if values are available for it.
   *
   * @return the MetadataField
   */
  def build(): Option[MetadataField]
}

