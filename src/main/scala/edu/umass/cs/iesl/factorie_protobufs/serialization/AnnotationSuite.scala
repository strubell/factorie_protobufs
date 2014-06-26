package edu.umass.cs.iesl.factorie_protobufs.serialization

import scala.collection.mutable
import cc.factorie.app.nlp.{Token, Document}
import edu.umass.cs.iesl.protos._
import scala.collection.JavaConverters._

/**
 * @author John Sullivan
 */

class AnnotationSuite(val annotators:IndexedSeq[AnnotationTemplate[_ ,_]]) {
  private implicit val classMap = mutable.HashMap[String, Class[_]]().withDefault{ className => Class.forName(className) }
  private val nameMap = mutable.HashMap[Class[_], String]().withDefault{ cl => cl.getName }
  private val annotatorMap = annotators.map(a => a.annotation -> a).toMap

  private def partitionAnnotators(annotators:TraversableOnce[AnnotationTemplate[_, _]]) =
    annotators.foldLeft((null.asInstanceOf[TokenAnnotation.type], mutable.ArrayBuffer[TokenTagAnnotation](), mutable.ArrayBuffer[CompoundAnnotation]()))
    { case((tokenAnno, tagAnnos, compAnnos), anno) =>
      anno match {
        case TokenAnnotation => (anno, tagAnnos, compAnnos)
        case tagAnno:TokenTagAnnotation => (tokenAnno, tagAnnos += tagAnno, compAnnos)
        case compAnno:CompoundAnnotation => (tokenAnno, tagAnnos, compAnnos += compAnno)
      }
    }

  def serialize(fDoc:Document, sDoc:DocumentBuilder = protoDocument):ProtoDocument = {
    sDoc.setId(fDoc.name)
    sDoc.setText(fDoc.string)

    val presentAnnotations = fDoc.annotators.flatMap {
      case(annotationClass, annotatorClass) if annotatorMap.contains(nameMap(annotationClass)) =>
        Some(annotatorMap(nameMap(annotationClass)).withAnnotator(nameMap(annotatorClass)))
      case ((annoClass, _), _) => println("WARNING: Document %s had annotation %s with no corresponding serializer".format(fDoc.name, nameMap(annoClass))); None
    }.zipWithIndex.map{case (anno, idx) => anno.withMethodIndex(idx)}

    sDoc.addAllMethod(presentAnnotations.map {_._2.method}.asJava)

    val (tokenAnno, tagAnnos, clusterAnnos) = partitionAnnotators(presentAnnotations)

    val sTokens = fDoc.tokens.map { fToken =>
      tokenAnno.serialize(fToken).addAllAnnotation(tagAnnos.map(_.serialize(fToken)).asJava).build()
    }.asJava

    sDoc.addAllToken(sTokens)

    val compounds = clusterAnnos.map(_.serialize(fDoc).build()).asJava

    sDoc.addAllCompound(compounds)
    sDoc.build()
  }

  def deserialize(sDoc:ProtoDocument, fDoc:Document = new Document()):Document = {
    fDoc.setName(sDoc.getId)
    fDoc.appendString(sDoc.getText)

    val presentAnnotations = sDoc.getMethodList.asScala.zipWithIndex.flatMap {
      case (method, idx) if annotatorMap.contains(method.getAnnotation) => Some(annotatorMap(method.getAnnotation).withMethodIndex(idx).withAnnotator(method.getAnnotator))
      case (method, _) => println("WARNING: Document %s had annotation %s with no corresponding deserializer".format(sDoc.getId, method.getAnnotation)); None
    }.toIndexedSeq

    fDoc.annotators ++= presentAnnotations.map (anno => classMap(anno.annotation) -> classMap(anno.annotator))

    val (tokenAnno, ta, ca) = partitionAnnotators(presentAnnotations)
    val tagAnnos = ta.map(a => a.methodIndex -> a).toMap
    val clusterAnnos = ca.map(a => a.methodIndex -> a).toMap

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
