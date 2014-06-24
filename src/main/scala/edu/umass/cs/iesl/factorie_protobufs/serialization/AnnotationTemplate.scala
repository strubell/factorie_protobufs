package edu.umass.cs.iesl.factorie_protobufs.serialization

import com.google.protobuf.Message
import edu.umass.cs.iesl.protos._
import cc.factorie.app.nlp.{TokenSpan, TokenString, Token, Document}
import scala.reflect.ClassTag
import scala.collection.JavaConverters._
import cc.factorie.app.nlp.coref.WithinDocCoref
import cc.factorie.app.nlp.phrase.Phrase

/**
 * @author John Sullivan
 */
object AnnotationTemplate {
  sealed trait Bool
  abstract class TRUE extends Bool
  abstract class FALSE extends Bool

  //def apply[PM <: Message, FI](annotator:AnnotationTemplate[PM, FI]) = new AnnotationBuilder[FALSE, FALSE, PM, FI](annotator)

  def setUp[PM <: Message, FI](worker:AnnotationTemplate[PM, FI]) = new AnnotationBuilder[FALSE, FALSE, PM, FI](annotator)

  implicit class BuildableAnnotation[PM <: Message, FI](anno:AnnotationBuilder[TRUE, TRUE, PM, FI]) {
    def build = new RealizedAnnotation(anno)
  }

  private case class AnnotationBuilder[MethodIndex <: Bool, AnnotatorString <: Bool, PM <: Message, FI]
  (worker:AnnotationTemplate[PM, FI], annotatorString:String, idx:Int) {

    this(_worker:AnnotationTemplate) = this[FALSE, FALSE](_worker, null, null)

    def withMethodIndex(_idx:Int) = new AnnotationBuilder[TRUE, AnnotatorString, PM, FI](worker, annotatorString, _idx)
    def withAnnotator(_annotatorString:String) = new AnnotationBuilder[MethodIndex, TRUE, PM, FI](worker, _annotatorString, idx)
  }

  private class RealizedAnnotation[PM <: Message, FI](builder:AnnotationBuilder[TRUE, TRUE, PM, FI]) {
    val AnnotationBuilder(worker, annotator, idx) = builder

    val serialize = worker.serialize(idx, annotator)
    val deserialize = worker.deserialize(idx, annotator)
  }
}

trait AnnotationTemplate[Serialized, Unserialized] {
  protected var methodIndex:Int = null
  protected var annotator:String = null
  def annotation:String
  def annotationType:AnnotationType

  def withMethod(_methodIndex:Int) = {
    methodIndex = _methodIndex
    this
  }
  def withAnnotator(_annotator:String) = {
    annotator = _annotator
    this
  }

  def serialize(un:Unserialized):Serialized
  def deserialize(ser:Serialized, un:Unserialized):Unserialized
}

trait CompoundAnnotation extends AnnotationTemplate[ProtoCompoundGroup, Document] {
  val annotationType = AnnotationType.CLUSTER

  def serialize(un: Document) = protoCompoundGroup.setMethodIndex(methodIndex).setType(annotationType)
}

object CorefAnnotation extends CompoundAnnotation {
  val annotation = "cc.factorie.app.nlp.coref.WithinDocCoref"

  override def serialize(un:Document) = super.serialize(un).addAllCompound{
    un.coref.entities.map { entity =>
      protoCompound.setText(if (entity.canonicalName != null) entity.canonicalName else entity.getFirstMention.string) // Come on people, it's 2014, I shouldn't have to guard for this.
      .addAllSlot(entity.mentions.map { mention =>
        protoSlot.setStartToken(mention.phrase.start).setEndToken(mention.phrase.end).build()
      }.asJava).build()
    }.asJava
  }.build()

  def deserialize(ser: ProtoCompoundGroup, un: Document) = {
    val coref = new WithinDocCoref(un)
    ser.getCompoundList.asScala.foreach{ sEnt =>
      sEnt.getSlotList.asScala.foldLeft(coref.newEntity()){ case(ent, sMent) =>
        coref.addMention(new Phrase(new TokenSpan(doc.asSection, sMent.getStartToken, sMent.getEndToken)), ent)
        ent
      }
    }
    un
  }
}

object TokenAnnotation extends AnnotationTemplate[ProtoToken, Token] {
  val annotation = "cc.factorie.app.nlp.Token"
  val annotationType = AnnotationType.TEXT
  def deserialize(ser: ProtoToken, un: Token) = new Token(ser.getStart, ser.getEnd)
  def serialize(un: Token) = protoToken.setStart(un.stringStart).setEnd(un.stringEnd)
}

trait TokenTagAnnotation extends AnnotationTemplate[ProtoAnnotation, Token] {
  val annotationType = AnnotationType.TAG

  abstract def serialize(un: Token) = protoAnnotation.setMethodIndex(methodIndex).setType(annotationType)
}

class NormalizedStringAnnotation[TokenStr <: TokenString](constructor:(Token, String) => TokenString)(implicit ct:ClassTag[TokenStr]) extends TokenTagAnnotation {
  val annotation = ct.runtimeClass.getName
  def deserialize(ser: ProtoAnnotation, un: Token) = un.attr += constructor(un, ser.getText)
  override def serialize(un: Token) = super.serialize(un).setText(un.attr[TokenStr].value)
}