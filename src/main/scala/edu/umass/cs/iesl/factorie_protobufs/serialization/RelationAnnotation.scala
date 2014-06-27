package edu.umass.cs.iesl.factorie_protobufs.serialization

import cc.factorie.app.nlp._
import edu.umass.cs.iesl.protos._
import cc.factorie.app.nlp.{TokenSpan, RelationMention, Document}
import scala.collection.JavaConverters._
import cc.factorie.app.nlp.phrase.Phrase

/**
 * @author John Sullivan
 */
//class RelationMentionList extends collection.mutable.ArrayBuffer[RelationMention] with Attr

object RelationAnnotation extends CompoundAnnotation {
  val annotation = classOf[RelationMentionList].getName
  private val ARG1 = "arg1"
  private val ARG2 = "arg2"

  def serialize(un: Document) = protoCompoundGroup.mergeFrom(methodAnno).addAllCompound {
    un.attr[RelationMentionList].map { rm =>
        protoCompound.setText(rm.evidence).setText(rm.relationType).addAllSlot{
          Seq(protoSlot.setLabel(ARG1).setStartToken(rm.arg1.phrase.start).setEndToken(rm.arg1.phrase.end).build(),
          protoSlot.setLabel(ARG2).setStartToken(rm.arg2.phrase.start).setEndToken(rm.arg2.phrase.end).build()).asJava
        }.build()
      }.asJava
  }.build()

  def deserialize(ser: ProtoCompoundGroup, un: Document) = {
    val rms = new RelationMentionList
    val coref = un.coref
    rms ++= ser.getCompoundList.asScala.map { sRel =>
      val evidence = sRel.getText
      val relationType = sRel.getType
      val arg1 = coref.addMention(new Phrase(new TokenSpan(un.asSection, sRel.getSlot(0).getStartToken, sRel.getSlot(0).getEndToken)))
      val arg2 = coref.addMention(new Phrase(new TokenSpan(un.asSection, sRel.getSlot(1).getStartToken, sRel.getSlot(1).getEndToken)))
      new RelationMention(arg1, arg2, relationType, evidence)
    }
    un.attr += rms
    un
  }
}