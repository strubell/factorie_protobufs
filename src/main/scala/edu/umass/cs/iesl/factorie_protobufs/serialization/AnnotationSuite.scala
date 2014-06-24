package edu.umass.cs.iesl.factorie_protobufs.serialization

import scala.collection.mutable
import cc.factorie.app.nlp.{Token, Document}
import edu.umass.cs.iesl.protos._
import scala.collection.JavaConverters._

/**
 * @author John Sullivan
 */
/*
class Anno(val annotators:IndexedSeq[AnnotationMethod]) {

  private implicit val annoMap = annotators.groupBy(_.annotation).mapValues(_.head)

  def deserialize(sDoc:ProtoDocument, fDoc:Document = new Document):Document = {
    val methods = sDoc.getMethodList.asScala.map(Method.fromProto).toIndexedSeq

    fDoc.setName(sDoc.getId).appendString(sDoc.getText)
  }
}
*/

class AnnotationSuite(val annotators:IndexedSeq[AnnotationMethod]) {
  private implicit val classMap = mutable.HashMap[String, Class[_]]().withDefault{ className => Class.forName(className) }
  private val nameMap = mutable.HashMap[Class[_], String]().withDefault{ cl => cl.getName }
  private val annotatorMap = annotators.map(a => a.annotation -> a).toMap

  //private val (tokenAnnotators, generalAnnotators) = annotators.partition(_.isInstanceOf[TokenLevelAnnotation])
  /*
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
    val presentAnnotations = annotatorMap.collect{case (annotation, _) if fDoc.annotators.contains(classMap(annotation)) => annotation}.toSet

    // adding all method labels
    annotators.filter(anno => presentAnnotations.contains(anno.annotation)).foreach(_.addMethod(sDoc))

    val pTokens = fDoc.tokens.map { fToken =>
      tokenAnnotators.filter(a => presentAnnotations.contains(a.annotation)).foldLeft(protoToken){case (pToken, anno) =>
        anno.asInstanceOf[TokenLevelAnnotation].serializeToken(fToken, pToken)
      }.build()
    }.asJava
    //tokenAnnotators.filter(a => presentAnnotations.contains(a.annotation)).foreach{_.addMethod(sDoc)}
    sDoc.addAllToken(pTokens)
    generalAnnotators.filter(a => presentAnnotations.contains(a.annotation)).foreach{ anno =>
      //anno.addMethod(sDoc)
      anno.serialize(fDoc, sDoc)
    }
    sDoc.build()
  }
  */

  def serialize(fDoc:Document, sDoc:DocumentBuilder = protoDocument):ProtoDocument = {
    sDoc.setId(fDoc.name)
    sDoc.setText(fDoc.string)

    val presentAnnotations = fDoc.annotators.zipWithIndex.flatMap {
      case((annotationClass, annotatorClass), idx) if annotatorMap.contains(nameMap(annotationClass)) =>
        Some(idx -> annotatorMap(nameMap(annotationClass)).withAnnotator(nameMap(annotatorClass)).withMethodIndex(idx))
      case ((annoClass, _), _) => println("WARNING: Document %s had annotation %s with no corresponding serializer".format(fDoc.name, nameMap(annoClass))); None
    }

    presentAnnotations.foreach {_._2.addMethod(sDoc)}

    val (ta, clusterAnnotations) = presentAnnotations.partition(_._2.isInstanceOf[TokenLevelAnnotation])
    val tokenAnnotations = ta.asInstanceOf[mutable.LinkedHashMap[Int, TokenLevelAnnotation]]

    val sTokens = fDoc.tokens.map { fToken =>
      tokenAnnotations.values.foldLeft(protoToken){case (sToken, anno) =>
        anno.serializeToken(fToken, sToken)
      }.build()
    }

    sDoc.addAllToken(sTokens.asJava)

    clusterAnnotations.values.foldLeft(sDoc){case (sD, anno) =>
      anno.serializeCompound(fDoc, sD)
    }.build()
  }


  def deserialize(sDoc:ProtoDocument, fDoc:Document = new Document()):Document = {
    fDoc.setName(sDoc.getId)
    fDoc.appendString(sDoc.getText)
    val presentAnnotations = sDoc.getMethodList.asScala.zipWithIndex.flatMap {
      case (method, idx) if annotatorMap.contains(method.getAnnotation) => Some(idx -> annotatorMap(method.getAnnotation).withMethodIndex(idx).withAnnotator(method.getAnnotator))
      case (method, _) => println("WARNING: Document %s had annotation %s with no corresponding deserializer".format(sDoc.getId, method.getAnnotation)); None
    }.toIndexedSeq

    presentAnnotations.foreach{ anno =>
      fDoc.annotators += classMap(anno.annotation) -> classMap(anno.annotator)
    }
    val (ta, ca) = presentAnnotations.partition(_._2.isInstanceOf[TokenLevelAnnotation])
    val tokenAnnotators = ta.toMap.asInstanceOf[Map[Int, TokenLevelAnnotation]]
    val clusterAnnotators = ca.toMap

    sDoc.getTokenList.asScala.foreach { sToken =>
      fDoc.asSection += sToken.getAnnotationList.asScala.foldLeft(null.asInstanceOf[Token]) { case(fToken, sAnno) =>
        tokenAnnotators(sAnno.getMethodIndex).deserializeAnnotatation(sAnno, fToken)
      }
    }
    sDoc.getCompoundList.asScala.foreach{ sCompound =>
      clusterAnnotators(sCompound.getMethodIndex).deserializeCompound(sCompound, fDoc)
    }
    fDoc
  }
}
