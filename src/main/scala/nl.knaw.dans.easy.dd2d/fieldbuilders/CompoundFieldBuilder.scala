package nl.knaw.dans.easy.dd2d.fieldbuilders

import nl.knaw.dans.easy.dd2d.mapping.JsonObject
import nl.knaw.dans.lib.dataverse.model.dataset.{ CompoundField, MetadataField }

import scala.collection.mutable

class CompoundFieldBuilder(name: String) extends AbstractFieldBuilder {
  private val values = new mutable.ListBuffer[JsonObject]

  def addValue(v: JsonObject): Unit = {
    values.append(v)
  }

  override def build(): MetadataField = {
    CompoundField(name, values.toList)
  }
}
