package edu.umass.cs.iesl.factorie_protobufs.serialization

import scala.collection.mutable
import cc.factorie.app.nlp.{Token, Document}
import edu.umass.cs.iesl.protos._
import scala.collection.JavaConverters._

/**
 * @author John Sullivan
 */
class AnnotationSuite(val annotators:IndexedSeq[AnnotationMethod]) {
  private implicit val classMap = mutable.HashMap[String, Class[_]]().withDefault{ className => Class.forName(className) }
  private val nameMap = mutable.HashMap[Class[_], String]().withDefault{ cl => cl.getName }
  private val annotatorMap = annotators.map(a => a.annotation -> a).toMap

  private val (tokenAnnotators, generalAnnotators) = annotators.partition(_.isInstanceOf[TokenLevelAnnotation])

  def serialize(fDoc:Document, sDoc:DocumentBuilder = protoDocument):ProtoDocument = {
    sDoc.setId(fDoc.name)
    sDoc.setText(fDoc.string)

    var idx = 0
    fDoc.annotators.foreach { case(annotationClass, annotatorClass) =>
      val annotation = nameMap(annotationClass); val annotator = nameMap(annotatorClass)
      annotatorMap.get(annotation) match {
        case Some(anno) =>
          val a = anno.withAnnotator(annotator).withMethodIndex(idx)
          idx += 1
          a
        case None => println("WARNING: Document %s had annotation %s with no corresponding serializer".format(fDoc.name, annotation))
      }
    }
    val presentAnnotations = annotatorMap.flatMap { case (annotation, _) =>
      fDoc.annotators.get(classMap(annotation)) match {
        case Some(_) => Some(annotation)
        case None => None
      }
    }.toSet

    println(s"Serializing annotations: ${presentAnnotations.mkString(",")}")

    val pTokens = fDoc.tokens.map { fToken =>
      tokenAnnotators.filter(a => presentAnnotations.contains(a.annotation)).foldLeft(protoToken){case (pToken, anno) =>
        anno.asInstanceOf[TokenLevelAnnotation].serializeToken(fToken, pToken)
      }.build()
    }.asJava
    tokenAnnotators.filter(a => presentAnnotations.contains(a.annotation)).foreach{_.addMethod(sDoc)}
    sDoc.addAllToken(pTokens)
    generalAnnotators.filter(a => presentAnnotations.contains(a.annotation)).foreach{ anno =>
      anno.addMethod(sDoc)
      anno.serialize(fDoc, sDoc)
    }
    sDoc.build()
  }

  def deserialize(sDoc:ProtoDocument, fDoc:Document = new Document()):Document = {
    fDoc.setName(sDoc.getId)
    fDoc.appendString(sDoc.getText)
    val presentAnnotators = sDoc.getMethodList.asScala.flatMap { method =>
      val am = annotatorMap.get(method.getAnnotation)
      if(am.isDefined) am.get.annotator = method.getAnnotator
      am
    }
    val presentTokenAnnotators = presentAnnotators.collect{case a:TokenLevelAnnotation => a}
    presentAnnotators.foreach { annoMethod =>

      fDoc.annotators += classMap(annoMethod.annotation) -> classMap(annoMethod.annotator)
    }
    sDoc.getMethodList.asScala.zipWithIndex.foreach { case(sMethod, idx) =>
      annotatorMap.get(sMethod.getAnnotation) match {
        case Some(anno) if anno.isInstanceOf[TokenLevelAnnotation] => anno.asInstanceOf[TokenLevelAnnotation].withMethodIndex(idx)
        case Some(_) => Unit
        case None => println("WARNING: deserializing %s. No annotation method found for %s".format(sDoc.getId, sMethod.getAnnotation))
      }
    }
    sDoc.getTokenList.asScala.foreach { sToken =>
      fDoc.asSection += presentTokenAnnotators.foldLeft(null.asInstanceOf[Token]) { case(fToken, anno) =>
        anno.asInstanceOf[TokenLevelAnnotation].deserializeToken(sToken, fToken)
      }
    }
    fDoc
  }
}
