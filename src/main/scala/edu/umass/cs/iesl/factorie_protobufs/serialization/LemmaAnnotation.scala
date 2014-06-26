package edu.umass.cs.iesl.factorie_protobufs.serialization

import cc.factorie.app.nlp.lemma._
import cc.factorie.app.nlp.Token
import scala.reflect.ClassTag
import edu.umass.cs.iesl.protos._

/**
 * @author John Sullivan
 */
class LemmaAnnotation[Lemma <: TokenLemma](constructor:((Token, String) => Lemma))(implicit ct:ClassTag[Lemma]) extends TokenTagAnnotation {
  val annotation = ct.getClass.getName

  override def serialize(un: Token) = super.serialize(un).setText(if(un.attr.contains[Lemma]) un.attr[Lemma].value else "").build()
  def deserialize(ser: ProtoAnnotation, un: Token) = {
    if(ser.getText.nonEmpty) {
      un.attr += constructor(un, ser.getText)
    }
    un
  }
}

object SimplifyDigitsLemmaAnnotation extends LemmaAnnotation({(t:Token, s:String) => new SimplifyDigitsTokenLemma(t, s)})
object CollapseDigitsLemmaAnnotation extends LemmaAnnotation({(t:Token, s:String) => new CollapseDigitsTokenLemma(t, s)})
object LowercaseLemmaAnnotation extends LemmaAnnotation({(t:Token, s:String) => new LowercaseTokenLemma(t, s)})
object PorterLemmaAnnotation extends LemmaAnnotation({(t:Token, s:String) => new PorterTokenLemma(t, s)})
object WordnetLemmaAnnotation extends LemmaAnnotation({(t:Token, s:String) => new WordNetTokenLemma(t, s)})
object GeneralLemmaAnnotation extends LemmaAnnotation({(t:Token, s:String) => new TokenLemma(t, s)})