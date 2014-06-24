package edu.umass.cs.iesl.factorie_protobufs.serialization

import edu.umass.cs.iesl.protos._
import cc.factorie.app.nlp.{TokenSpan, RelationMention, Document}
import scala.collection.JavaConverters._
import cc.factorie.util.Attr
import cc.factorie.app.nlp.phrase.Phrase

/**
 * @author John Sullivan
 */
class RelationMentionList extends collection.mutable.ArrayBuffer[RelationMention] with Attr

class RelationAnnotation extends AnnotationMethod {
  val annotation = "cc.factorie.app.nlp.coref.RelationMentionList"
  val annotationType = AnnotationType.CLUSTER

  private val ARG1 = "arg1"
  private val ARG2 = "arg2"


  def serialize(doc: Document, serDoc: DocumentBuilder) = serDoc.addMethod(protoMethod)
    .addCompound(protoCompoundGroup.setType(annotationType).setMethodIndex(_methodIndex)
    .addAllCompound(doc.attr[RelationMentionList].map { rm:RelationMention =>
    protoCompound.setText(rm.evidence).setType(rm.relationType)
      .addAllSlot(Seq(protoSlot.setLabel(ARG1).setStartToken(rm.arg1.phrase.start).setEndToken(rm.arg1.phrase.end).build(),
      protoSlot.setLabel(ARG2).setStartToken(rm.arg2.phrase.start).setEndToken(rm.arg2.phrase.end).build()).asJava).build()
    }.asJava).build())

  def deserialize(doc: Document, serDoc: ProtoDocument)(annoClass: (Class[_], Class[_])) = {
    val rms = new RelationMentionList
    doc.annotators += annoClass
    val coref = doc.coref
    rms ++= serDoc.getCompound(_methodIndex).getCompoundList.asScala.map{ sRel =>
      val evidence = sRel.getText
      val relationType = sRel.getType
      val arg1 = coref.addMention(new Phrase(new TokenSpan(doc.asSection, sRel.getSlot(0).getStartToken, sRel.getSlot(0).getEndToken)))
      val arg2 = coref.addMention(new Phrase(new TokenSpan(doc.asSection, sRel.getSlot(1).getStartToken, sRel.getSlot(1).getEndToken)))
      new RelationMention(arg1, arg2, relationType, evidence)
    }
    doc.attr += rms
    doc
  }
}
