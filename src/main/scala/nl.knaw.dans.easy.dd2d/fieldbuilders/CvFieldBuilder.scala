package nl.knaw.dans.easy.dd2d.fieldbuilders

import nl.knaw.dans.lib.dataverse.model.dataset.{ ControlledMultipleValueField, MetadataField }

import scala.collection.mutable

class CvFieldBuilder(name: String) extends AbstractFieldBuilder {
  private val values = new mutable.ListBuffer[String]

  def addValue(v: String): Unit = {
    values.append(v)
  }

  override def build(): MetadataField = {
    ControlledMultipleValueField(name, values.toList)
  }
}
