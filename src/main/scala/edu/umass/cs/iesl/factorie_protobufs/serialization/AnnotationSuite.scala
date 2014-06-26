package edu.umass.cs.iesl.factorie_protobufs.serialization

import scala.collection.mutable
import cc.factorie.app.nlp.Document
import edu.umass.cs.iesl.protos._
import scala.collection.JavaConverters._

/**
 * @author John Sullivan
 */

class AnnotationSuite(tokenAnno:TokenAnnotation.type , tagAnnos:Seq[TokenTagAnnotation], clusterAnnos:Seq[CompoundAnnotation]){//val annotators:IndexedSeq[AnnotationTemplate[_, _]]) {
  private implicit val classMap = mutable.HashMap[String, Class[_]]().withDefault{ className => Class.forName(className) }
  private val nameMap = mutable.HashMap[Class[_], String]().withDefault{ cl => cl.getName }
  private val tagMap = tagAnnos.map(a => a.annotation -> a).toMap
  private val clusterMap = clusterAnnos.map(a => a.annotation -> a).toMap

  def serialize(fDoc:Document, sDoc:DocumentBuilder = protoDocument):ProtoDocument = {
    sDoc.setId(fDoc.name)
    sDoc.setText(fDoc.string)

    val presentAnnotations = fDoc.annotators.flatMap {
      case (annotationClass, annotatorClass) if tokenAnno.annotation == nameMap(annotationClass) =>
        Some(tokenAnno.withAnnotator(nameMap(annotatorClass)))
      case (annotationClass, annotatorClass) if tagMap.keySet.contains(nameMap(annotationClass)) =>
        Some(tagMap(nameMap(annotationClass)).withAnnotator(nameMap(annotatorClass)))
      case (annotationClass, annotatorClass) if clusterMap.keySet.contains(nameMap(annotationClass)) =>
        Some(clusterMap(nameMap(annotationClass)).withAnnotator(nameMap(annotatorClass)))
      case (annoClass, _) => println("WARNING: Document %s had annotation %s with no corresponding serializer".format(fDoc.name, nameMap(annoClass))); None
    }.zipWithIndex.map{case (anno, idx) => anno.withMethodIndex(idx)}

    val presentAnnos = presentAnnotations.map(_.annotation).toSet

    sDoc.addAllMethod(presentAnnotations.map {_.method}.asJava)

    fDoc.tokens.foreach { fToken =>
      val sToken = tokenAnno.serialize(fToken)
      tagAnnos.filter(a => presentAnnos.contains(a.annotation)).foreach{ anno =>
        sToken.addAnnotation(anno.serialize(fToken))
      }
      sDoc.addToken(sToken.build())
    }

    clusterAnnos.filter(a => presentAnnos.contains(a.annotation)).foreach{ anno =>
      sDoc.addCompound(anno.serialize(fDoc))
    }

    sDoc.build()
  }

  def deserialize(sDoc:ProtoDocument, fDoc:Document = new Document()):Document = {
    fDoc.setName(sDoc.getId)
    fDoc.appendString(sDoc.getText)

    var tokenAnno = TokenAnnotation
    val tas = mutable.ArrayBuffer[TokenTagAnnotation]()
    val cas = mutable.ArrayBuffer[CompoundAnnotation]()

    sDoc.getMethodList.asScala.zipWithIndex.foreach {
      case (method, idx) if tokenAnno.annotation == method.getAnnotation =>
        tokenAnno = tokenAnno.withMethodIndex(idx).withAnnotator(method.getAnnotator).asInstanceOf[TokenAnnotation.type]
      case (method, idx) if tagMap.keySet.contains(method.getAnnotation) =>
        tas += tagMap(method.getAnnotation).withMethodIndex(idx).withAnnotator(method.getAnnotator).asInstanceOf[TokenTagAnnotation]
      case (method, idx) if clusterMap.keySet.contains(method.getAnnotation) =>
        cas += clusterMap(method.getAnnotation).withMethodIndex(idx).withAnnotator(method.getAnnotator).asInstanceOf[CompoundAnnotation]
      case (method, _) => println("WARNING: Document %s had annotation %s with no corresponding deserializer".format(sDoc.getId, method.getAnnotation))
    }

    fDoc.annotators += classMap(tokenAnno.annotation) -> classMap(tokenAnno.annotator)
    fDoc.annotators ++= tas.map(anno => classMap(anno.annotation) -> classMap(anno.annotator))
    fDoc.annotators ++= cas.map(anno => classMap(anno.annotation) -> classMap(anno.annotator))

    val tokenAnnos = tas.map(a => a.methodIndex -> a).toMap
    val compAnnos = cas.map(a => a.methodIndex -> a).toMap

    fDoc.asSection ++= sDoc.getTokenList.asScala.map { sToken =>
       sToken.getAnnotationList.asScala.foldLeft(tokenAnno.deserialize(sToken)) { case(fToken, sAnno) =>
        tokenAnnos(sAnno.getMethodIndex).deserialize(sAnno, fToken)
      }
    }

    //sadly, this relies on side-effects to add these annotations to the attr list of the doc.
    sDoc.getCompoundList.asScala.foreach { sComp =>
      compAnnos(sComp.getMethodIndex).deserialize(sComp, fDoc)
    }
    fDoc
  }
}
