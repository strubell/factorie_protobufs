package edu.umass.cs.iesl.factorie_protobufs.serialization

import edu.umass.cs.iesl.protos._
import cc.factorie.app.nlp.{BasicSection, Section, Document, Sentence}
import scala.collection.JavaConverters._

/**
 * @author John Sullivan
 */
trait BoundaryAnnotation extends CompoundAnnotation {
  override val annotationType = AnnotationType.BOUNDARY
}

object SentenceAnnotation extends BoundaryAnnotation {
  val annotation = classOf[Sentence].getName

  def deserialize(ser: ProtoCompoundGroup, un: Document) = {
    ser.getCompound(0).getSlotList.asScala.foreach { sSlot =>
      new Sentence(un.asSection, sSlot.getStartToken, sSlot.getEndToken) // factorie side-effects!
    }
    un
  }

  override def serialize(un:Document) = protoCompoundGroup.mergeFrom(methodAnno).addCompound{
    protoCompound.addAllSlot{
      un.sentences.map { fSentence =>
        protoSlot.setStartToken(fSentence.start).setEndToken(fSentence.end).build()
      }.asJava
    }.build()
  }.build()
}

object SectionAnnotation extends BoundaryAnnotation {
  val annotation = classOf[Section].getName

  override def serialize(un: Document) = protoCompoundGroup.mergeFrom(methodAnno).addCompound{
    protoCompound.addAllSlot{
      un.sections.map { fSection =>
      // These are actually character offsets into the docstring rather than token offsets,but we should be safe in this case
        protoSlot.setStartToken(fSection.stringStart).setEndToken(fSection.stringEnd).build()
      }.asJava
    }.build()
  }.build()

  def deserialize(ser: ProtoCompoundGroup, un: Document) = {
    ser.getCompound(0).getSlotList.asScala.foreach{ sSlot =>
      un += new BasicSection(un, sSlot.getStartToken, sSlot.getEndToken)
    }
    un
  }
}