package edu.umass.cs.iesl.factorie_protobufs.serialization

import edu.umass.cs.iesl.protos._
import cc.factorie.app.nlp.{Token => FacToken, TokenString}
import scala.reflect.ClassTag
import cc.factorie.app.nlp.segment.{OntonotesNormalizedTokenString, PlainNormalizedTokenString}

/**
 * @author John Sullivan
 */
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

object PlainNormalizedTokenAnnotation extends NormalizedTokenAnnotation({(t, s) => new PlainNormalizedTokenString(t, s)})
object OntonotesNormalizedTokenAnnotation extends NormalizedTokenAnnotation({(t, s) => new OntonotesNormalizedTokenString(t, s)})