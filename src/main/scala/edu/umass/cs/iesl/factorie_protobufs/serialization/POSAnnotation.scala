package edu.umass.cs.iesl.factorie_protobufs.serialization

import cc.factorie.app.nlp.pos.PennPosTag
import cc.factorie.app.nlp.Token
import edu.umass.cs.iesl.protos._

/**
 * @author John Sullivan
 */
object POSAnnotation extends TokenTagAnnotation {
  val annotation = classOf[PennPosTag].getName
  println("in class:%s annotation is %s".format(this.getClass.getName, annotation))

  override def serialize(un: Token) = protoAnnotation.mergeFrom(methodAnno).setText(un.posTag.categoryValue).build()
  def deserialize(ser: ProtoAnnotation, un: Token) = {
    un.attr += new PennPosTag(un, ser.getText)
    un
  }
}