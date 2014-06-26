package edu.umass.cs.iesl.factorie_protobufs.serialization

import edu.umass.cs.iesl.protos._
import cc.factorie.app.nlp.{Token, TokenString}
import scala.reflect.ClassTag
import cc.factorie.app.nlp.segment.{OntonotesNormalizedTokenString, PlainNormalizedTokenString}

/**
 * @author John Sullivan
 */
class NormalizedTokenAnnotation[TokenStr <: TokenString](constructor:((Token, String) => TokenStr))(implicit ct:ClassTag[TokenStr]) extends TokenTagAnnotation {
  val annotation = ct.runtimeClass.getName

  override def serialize(un: Token) = protoAnnotation.mergeFrom(methodAnno).setText(if(un.attr.contains[TokenStr]) un.attr[TokenStr].value else "").build()
  def deserialize(ser: ProtoAnnotation, un: Token) = {
    if(ser.getText.nonEmpty) {
      un.attr += constructor(un, ser.getText)
    }
    un
  }
}

object PlainNormalizedTokenAnnotation extends NormalizedTokenAnnotation({(t, s) => new PlainNormalizedTokenString(t, s)})
object OntonotesNormalizedTokenAnnotation extends NormalizedTokenAnnotation({(t, s) => new OntonotesNormalizedTokenString(t, s)})