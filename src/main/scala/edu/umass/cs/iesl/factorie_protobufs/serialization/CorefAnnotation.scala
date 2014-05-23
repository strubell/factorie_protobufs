package edu.umass.cs.iesl.factorie_protobufs.serialization

import edu.umass.cs.iesl.protos._
import cc.factorie.app.nlp.{TokenSpan, Document}

import scala.collection.JavaConverters._
import cc.factorie.app.nlp.coref.WithinDocCoref
import cc.factorie.app.nlp.phrase.Phrase

/**
 * @author John Sullivan
 */
class CorefAnnotation extends AnnotationMethod {
  val annotation = "cc.factorie.app.nlp.coref.WithinDocCoref"
  val annotationType = AnnotationType.CLUSTER

  def serialize(doc:Document, serDoc:DocumentBuilder):DocumentBuilder = serDoc.addMethod(protoMethod).addCompound(protoCompoundGroup.setType(annotationType).setMethodIndex(_methodIndex).addAllCompound(doc.coref.entities.map { entity =>
    protoCompound.setText(entity.canonicalName).addAllSlot(entity.mentions.map { mention =>
      protoSlot.setStartToken(mention.phrase.start).setEndToken(mention.phrase.end).build()
    }.asJava).build()
  }.asJava).build())

  def deserialize(doc:Document, serDoc:ProtoDocument)(annoClass: (Class[_], Class[_])): Document = {
    val coref = new WithinDocCoref(doc)
    doc.annotators += annoClass
    serDoc.getCompound(_methodIndex).getCompoundList.asScala.foreach{ sEnt =>
      sEnt.getSlotList.asScala.foldLeft(coref.newEntity()){ case(ent, sMent) =>
        coref.addMention(new Phrase(new TokenSpan(doc.asSection, sMent.getStartToken, sMent.getEndToken)), ent)
        ent
      }
    }
    doc
  }
}
