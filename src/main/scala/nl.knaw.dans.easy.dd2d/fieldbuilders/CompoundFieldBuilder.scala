package nl.knaw.dans.easy.dd2d.fieldbuilders

import nl.knaw.dans.easy.dd2d.mapping.JsonObject
import nl.knaw.dans.lib.dataverse.model.dataset.{ CompoundField, MetadataField }

import scala.collection.mutable

class CompoundFieldBuilder(name: String, multipleValues: Boolean = true) extends AbstractFieldBuilder {
  private val values = new mutable.ListBuffer[JsonObject]

  def addValue(v: JsonObject): Unit = {
    if (!multipleValues && values.nonEmpty) throw new IllegalArgumentException("Trying to add a second value to a single value field")
    values.append(v)
  }

  override def build(): Option[MetadataField] = {
    if (values.nonEmpty) Option(
      if (multipleValues) CompoundField(name, values.toList)
      else CompoundField(name, values.head))
    else Option.empty
  }
}
