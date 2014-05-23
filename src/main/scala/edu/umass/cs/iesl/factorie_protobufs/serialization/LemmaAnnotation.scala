package edu.umass.cs.iesl.factorie_protobufs.serialization

import cc.factorie.app.nlp.lemma._
import cc.factorie.app.nlp.Token
import scala.reflect.ClassTag
import edu.umass.cs.iesl.protos._

/**
 * @author John Sullivan
 */
class LemmaAnnotation[Lemma <: TokenLemma](constructor:((Token, String) => Lemma))(implicit m:ClassTag[Lemma]) extends TokenLevelAnnotation {
  val annotation:String = m.runtimeClass.getName
  val annotationType = AnnotationType.TEXT

  def serializeToken(fToken:Token, pToken:TokenBuilder) = {
    val pAnno = indexedAnnotation.setType(annotationType)
    if(fToken.attr.contains[Lemma]) {
      pAnno.setText(fToken.attr[Lemma].value)
    }
    pToken.addAnnotation(pAnno.build())
  }

  def deserializeToken(pToken:ProtoToken, fToken:Token) = {
    fToken.attr += constructor(fToken, pToken.getAnnotation(_methodIndex).getText)
    fToken
  }
}

object SimplifyDigitsLemmaAnnotation extends LemmaAnnotation({(t:Token, s:String) => new SimplifyDigitsTokenLemma(t, s)})
object CollapseDigitsLemmaAnnotation extends LemmaAnnotation({(t:Token, s:String) => new CollapseDigitsTokenLemma(t, s)})
object LowercaseLemmaAnnotation extends LemmaAnnotation({(t:Token, s:String) => new LowercaseTokenLemma(t, s)})
object PorterLemmaAnnotation extends LemmaAnnotation({(t:Token, s:String) => new PorterTokenLemma(t, s)})
object WordnetLemmaAnnotation extends LemmaAnnotation({(t:Token, s:String) => new WordNetTokenLemma(t, s)})
object GeneralLemmaAnnotation extends LemmaAnnotation({(t:Token, s:String) => new TokenLemma(t, s)})