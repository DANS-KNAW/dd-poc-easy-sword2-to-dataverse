package nl.knaw.dans.easy.dd2d.fieldbuilders

import nl.knaw.dans.lib.dataverse.model.dataset.MetadataField

abstract class AbstractFieldBuilder {
  abstract def build(): MetadataField
}

