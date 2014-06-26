package edu.umass.cs.iesl.factorie_protobufs.serialization

import edu.umass.cs.iesl.protos._
import cc.factorie.app.nlp.{TokenSpan, Document}

import scala.collection.JavaConverters._
import cc.factorie.app.nlp.coref.WithinDocCoref
import cc.factorie.app.nlp.phrase.Phrase

/**
 * @author John Sullivan
 */
object CorefAnnotation extends CompoundAnnotation {
  val annotation = classOf[WithinDocCoref].getName

  override def serialize(un:Document) = protoCompoundGroup.mergeFrom(methodAnno).addAllCompound{
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
        coref.addMention(new Phrase(new TokenSpan(un.asSection, sMent.getStartToken, sMent.getEndToken)), ent)
        ent
      }
    }
    un
  }
}
