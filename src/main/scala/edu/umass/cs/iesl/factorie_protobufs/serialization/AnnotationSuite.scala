package edu.umass.cs.iesl.factorie_protobufs.serialization

import scala.collection.mutable
import cc.factorie.app.nlp.{Token, Document}
import edu.umass.cs.iesl.protos._
import scala.collection.JavaConverters._

/**
 * @author John Sullivan
 */

class AnnotationSuite(tokenAnno:TokenAnnotation.type , tagAnnos:Seq[TokenTagAnnotation], clusterAnnos:Seq[CompoundAnnotation]){//val annotators:IndexedSeq[AnnotationTemplate[_, _]]) {
  private implicit val classMap = mutable.HashMap[String, Class[_]]().withDefault{ className => Class.forName(className) }
  private val nameMap = mutable.HashMap[Class[_], String]().withDefault{ cl => cl.getName }
  //private val annotatorMap = tokenAnno.annotation -> tokenAnno :+ tagAnnos.map(a => a.annotation -> a) ++ clusterAnnos.map(a => a.annotation -> a)
  private val tagMap = tagAnnos.map(a => a.annotation -> a).toMap
  private val clusterMap = clusterAnnos.map(a => a.annotation -> a).toMap
  //private val annotatorMap = annotators.map(a => a.annotation -> a).toMap
  /*
  private def partitionAnnotators(annotators:TraversableOnce[AnnotationTemplate[_, _]]) =
    annotators.foldLeft((null.asInstanceOf[TokenAnnotation.type], mutable.ArrayBuffer[TokenTagAnnotation](), mutable.ArrayBuffer[CompoundAnnotation]()))
    { case((tokenAnno, tagAnnos, compAnnos), anno) =>
      anno match {
        case tokenAnno:TokenAnnotation.type => (tokenAnno, tagAnnos, compAnnos)
        case tagAnno:TokenTagAnnotation => (tokenAnno, tagAnnos += tagAnno, compAnnos)
        case compAnno:CompoundAnnotation => (tokenAnno, tagAnnos, compAnnos += compAnno)
      }
    }
  */
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

    sDoc.addAllMethod(presentAnnotations.map {_.method}.asJava)

    //val (tokenAnno, tagAnnos, clusterAnnos) = partitionAnnotators(presentAnnotations)

    fDoc.tokens.foreach { fToken =>
      val sToken = tokenAnno.serialize(fToken)
      tagAnnos.foreach{ anno =>
        sToken.addAnnotation(anno.serialize(fToken))
      }
      sDoc.addToken(sToken.build())
    }

    clusterAnnos.foreach{ anno =>
      sDoc.addCompound(anno.serialize(fDoc))
    }

    sDoc.build()
  }

  def deserialize(sDoc:ProtoDocument, fDoc:Document = new Document()):Document = {
    fDoc.setName(sDoc.getId)
    fDoc.appendString(sDoc.getText)

    val presentAnnotations = sDoc.getMethodList.asScala.zipWithIndex.flatMap {
      case (method, idx) if tokenAnno.annotation == method.getAnnotation =>
        Some(tokenAnno.withMethodIndex(idx).withAnnotator(method.getAnnotator))
      case (method, idx) if tagMap.keySet.contains(method.getAnnotation) =>
        Some(tagMap(method.getAnnotation).withMethodIndex(idx).withAnnotator(method.getAnnotator))
      case (method, idx) if clusterMap.keySet.contains(method.getAnnotation) =>
        Some(clusterMap(method.getAnnotation).withMethodIndex(idx).withAnnotator(method.getAnnotator))
      case (method, _) => println("WARNING: Document %s had annotation %s with no corresponding deserializer".format(sDoc.getId, method.getAnnotation)); None
    }.toIndexedSeq

    fDoc.annotators ++= presentAnnotations.map (anno => classMap(anno.annotation) -> classMap(anno.annotator))

    //val (tokenAnno, ta, ca) = partitionAnnotators(presentAnnotations)
    //val tagAnnos = ta.map(a => a.methodIndex -> a).toMap
    //val clusterAnnos = ca.map(a => a.methodIndex -> a).toMap

    fDoc.asSection ++= sDoc.getTokenList.asScala.map { sToken =>
       sToken.getAnnotationList.asScala.foldLeft(tokenAnno.deserialize(sToken)) { case(fToken, sAnno) =>
        tagAnnos(sAnno.getMethodIndex).deserialize(sAnno, fToken)
      }
    }

    //sadly, this relies on side-effects to add these annotations to the attr list of the doc.
    sDoc.getCompoundList.asScala.foreach { sComp =>
      clusterAnnos(sComp.getMethodIndex).deserialize(sComp, fDoc)
    }
    fDoc
  }
}
