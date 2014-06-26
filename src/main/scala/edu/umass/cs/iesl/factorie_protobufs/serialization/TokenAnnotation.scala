package edu.umass.cs.iesl.factorie_protobufs.serialization

import edu.umass.cs.iesl.protos._
import cc.factorie.app.nlp.Token

/**
 * @author John Sullivan
 */
object TokenAnnotation extends AnnotationTemplate[ProtoToken, Token] {
  val annotation = classOf[Token].getName
  val annotationType = AnnotationType.TEXT
  def deserialize(ser: ProtoToken, un: Token = null) = new Token(ser.getStart, ser.getEnd)
  def serialize(un: Token) = protoToken.setStart(un.stringStart).setEnd(un.stringEnd)
}