package edu.umass.cs.iesl.factorie_protobufs.serialization

import edu.umass.cs.iesl.protos._
import cc.factorie.app.nlp._

/**
 * @author John Sullivan
 */
trait AnnotationTemplate[Serialized, Unserialized] {
  protected var _methodIndex = null.asInstanceOf[Int]
  protected var _annotator = null.asInstanceOf[String]
  def annotation:String
  final def annotator = _annotator
  final def methodIndex = _methodIndex
  def annotationType:AnnotationType

  def withMethodIndex(__methodIndex:Int) = {
    _methodIndex = __methodIndex
    this
  }
  def withAnnotator(__annotator:String) = {
    _annotator = __annotator
    this
  }
  lazy val method = protoMethod.setAnnotation(annotation).setType(annotationType).setAnnotator(_annotator).build()

  def serialize(un:Unserialized):Serialized

  def deserialize(ser:Serialized, un:Unserialized):Unserialized
}

trait CompoundAnnotation extends AnnotationTemplate[ProtoCompoundGroup, Document] {
  val annotationType = AnnotationType.CLUSTER


  protected def methodAnno = protoCompoundGroup.setMethodIndex(_methodIndex).setType(annotationType).build()
}

trait TokenTagAnnotation extends AnnotationTemplate[ProtoAnnotation, Token] {
  val annotationType = AnnotationType.TAG

  protected def methodAnno = protoAnnotation.setMethodIndex(_methodIndex).setType(annotationType).build()
}