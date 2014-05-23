package edu.umass.cs.iesl

import java.io.InputStream

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
  type TokenBuilder = ProtoFac.Document.Token.Builder
  type ProtoToken = ProtoFac.Document.Token

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
}
