package edu.umass.cs.iesl.factorie_protobufs.serialization

import edu.umass.cs.iesl.protos._
import cc.factorie.app.nlp.{Token, TokenString}
import scala.reflect.ClassTag
import cc.factorie.app.nlp.segment.{OntonotesNormalizedTokenString, PlainNormalizedTokenString}

/**
 * @author John Sullivan
 */
/*
class NormalizedTokenAnnotation[TokenStr <: TokenString](constructor:((FacToken, String) => TokenStr))(implicit m:ClassTag[TokenStr]) extends TokenLevelAnnotation {
  val annotation = m.runtimeClass.getName
  val annotationType = AnnotationType.TEXT

  def serializeToken(fToken:FacToken, pToken:TokenBuilder) = {
    val pAnno = indexedAnnotation.setType(annotationType)
    if(fToken.attr.contains[TokenStr]) {
      pAnno.setText(fToken.attr[TokenStr].value)
    }
    pToken.addAnnotation(pAnno.build())
    pToken
  }
  def deserializeToken(pToken:ProtoToken, fToken:FacToken) = {
    fToken.attr += constructor(fToken, pToken.getAnnotation(_methodIndex).getText)
    fToken
  }
}
*/
class NormalizedTokenAnnotation[TokenStr <: TokenString](constructor:((Token, String) => TokenStr))(implicit ct:ClassTag[TokenStr]) extends TokenTagAnnotation {
  val annotation = ct.getClass.getName


  override def serialize(un: Token) = super.serialize(un).setText(if(un.attr.contains[TokenStr]) un.attr[TokenStr].value else "").build()

  def deserialize(ser: ProtoAnnotation, un: Token) = {
    if(ser.getText.nonEmpty) {
      un.attr += constructor(un, ser.getText)
    }
    un
  }
}

object PlainNormalizedTokenAnnotation extends NormalizedTokenAnnotation({(t, s) => new PlainNormalizedTokenString(t, s)})
object OntonotesNormalizedTokenAnnotation extends NormalizedTokenAnnotation({(t, s) => new OntonotesNormalizedTokenString(t, s)})