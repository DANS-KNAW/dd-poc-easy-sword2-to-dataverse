package nl.knaw.dans.easy.dd2d.fieldbuilders
import nl.knaw.dans.lib.dataverse.model.dataset.{ ControlledMultipleValueField, ControlledSingleValueField, MetadataField, PrimitiveMultipleValueField, PrimitiveSingleValueField }

import scala.collection.mutable

class PrimitiveFieldBuilder(name: String, multipleValues: Boolean = true) extends AbstractFieldBuilder {
  private val values = new mutable.ListBuffer[String]

  def addValue(v: String): Unit = {
    if (!multipleValues && values.nonEmpty) throw new IllegalArgumentException("Trying to add a second value to a single value field")
    values.append(v)
  }

  override def build(): Option[MetadataField] = {
    if (values.nonEmpty)
      if (multipleValues) Option(PrimitiveMultipleValueField(name, values.toList))
      else Option(PrimitiveSingleValueField(name, values.head))
    Option.empty
  }
}
