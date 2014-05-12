package edu.umass.cs.iesl

/**
 * @author John Sullivan
 */
package object protos {
  type ProtoDocument = ProtoFac.Document
  type ProtoMethod = ProtoFac.Document.Method
  type ProtoAnnotationType = ProtoFac.Document.AnnotationType
  type ProtoAnnotation = ProtoFac.Document.Annotation
  type ProtoToken = ProtoFac.Document.Token

  object AnnotationType {
    val TAG = ProtoFac.Document.AnnotationType.TAG
    val OTHER = ProtoFac.Document.AnnotationType.OTHER
    val BOUNDARY = ProtoFac.Document.AnnotationType.BOUNDARY
  }

  def protoMethod = ProtoFac.Document.Method.newBuilder()
  def protoDocument = ProtoFac.Document.newBuilder()
  def protoAnnotation = ProtoFac.Document.Annotation.newBuilder()
  def protoToken = ProtoFac.Document.Token.newBuilder()
}
