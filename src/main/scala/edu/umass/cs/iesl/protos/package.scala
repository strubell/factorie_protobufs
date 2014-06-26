package edu.umass.cs.iesl

import java.io.InputStream
import scala.collection.mutable
import scala.collection.JavaConverters._
import cc.factorie.app.nlp.{Document => FacDocument}
import edu.umass.cs.iesl.factorie_protobufs.serialization.AnnotationMethod

/**
 * @author John Sullivan
 */
package object protos {
  type DocumentBuilder = ProtoFac.Document.Builder
  type ProtoDocument = ProtoFac.Document
  type MethodBuilder = ProtoFac.Document.Method.Builder
  type ProtoMethod = ProtoFac.Document.Method
  type AnnotationType = ProtoFac.Document.AnnotationType
  type AnnotationBuilder = ProtoFac.Document.Annotation.Builder
  type ProtoAnnotation = ProtoFac.Document.AnnotationOrBuilder
  type ProtoCompoundGroup = ProtoFac.Document.CompoundGroupOrBuilder
  //type TokenBuilder = ProtoFac.Document.Token.Builder
  type ProtoToken = ProtoFac.Document.TokenOrBuilder

  object AnnotationType {
    val TAG = ProtoFac.Document.AnnotationType.TAG
    val OTHER = ProtoFac.Document.AnnotationType.OTHER
    val BOUNDARY = ProtoFac.Document.AnnotationType.BOUNDARY
    val TEXT = ProtoFac.Document.AnnotationType.TEXT
    val CLUSTER = ProtoFac.Document.AnnotationType.CLUSTER
  }

  def protoMethod = ProtoFac.Document.Method.newBuilder()
  def protoDocument = ProtoFac.Document.newBuilder()
  def protoAnnotation = ProtoFac.Document.Annotation.newBuilder()
  def protoToken = ProtoFac.Document.Token.newBuilder()
  def protoCompoundGroup = ProtoFac.Document.CompoundGroup.newBuilder()
  def protoCompound = ProtoFac.Document.Compound.newBuilder()
  def protoSlot = ProtoFac.Document.Compound.CompoundSlot.newBuilder()

  def readDocument(is:InputStream):ProtoDocument = ProtoFac.Document.parseFrom(is)
 /*
  case class Method(annotation:String, annotator:String, annotationType:AnnotationType, anno:AnnotationMethod) {
    def toProto = protoMethod.setAnnotation(annotation).setAnnotator(annotator).setType(annotationType).build()
    lazy val annotationClass:Class[_] = Method.classMap(annotation)
    lazy val annotatorClass:Class[_] = Method.classMap(annotator)
  }
  object Method {
    private val classMap = mutable.HashMap[String, Class[_]]().withDefault(classString => Class.forName(classString))

    def fromProto(p:ProtoMethod)(implicit annoMap:Map[String, AnnotationMethod]):Method = Method(p.getAnnotation, p.getAnnotator, p.getType, anno(p.getAnnotation))
  }

  case class Document(id:String, text:String, methods:IndexedSeq[Method]) {
    def toProto = protoDocument.setId(id).setText(text).addAllMethod(methods.map(_.toProto).asJava)
    def toFactorie(doc:FacDocument = new FacDocument()):FacDocument = {
      doc.setName(id).appendString(text)
    }
  }

  object Document {
    def fromProto(p:ProtoDocument)(implicit anno:Map[String, AnnotationMethod]):Document = Document(p.getId, p.getText, p.getMethodList.asScala.map(Method.fromProto))
  }
  */
}
