package edu.umass.cs.iesl.fac_reader

import edu.umass.cs.iesl.protos._
import cc.factorie.app.nlp._
import scala.collection.JavaConverters._


/**
 * Hello world!
 *
 */
/*
trait SerDe[Format] {
  def serialize(doc:Document):Format
  def deserialize(ser:Format):Document
}

object Proto extends SerDe[ProtoDocument] {
  /*
  def annotationToMethod(annoPair:(Class[_], Class[_])) = {
    val (anno, annotator) = annoPair
    val name = annotator.getSimpleName
    val method = P.Document.Method.newBuilder().setId(name)
    anno match {
      case _:Class[pos.PennPosTag] => method.setType(P.Document.AnnotationType.TAG)
      case _:Class[parse.ParseTree] => method.setType(P.Document.AnnotationType.SYNTAX_HEAD)
      case _:Class[Token] => method.setType(P.Document.AnnotationType)
    }
    method.build()
  }
  */
  /*
  def getTokens(doc:Document) = doc.tokens.map{ token =>
      P.Document.Token.newBuilder().setStart(doc.stringStart).setEnd(doc.stringEnd).build() //todo Annotations on Token!!
    }.asJava
  */

  def getTokens(doc:Document, tokenAnnos:Iterable[TokenAnnotation]) = doc.tokens.map{token =>
    val pToken = protoToken.setStart(token.stringStart).setEnd(token.stringEnd)
    tokenAnnos.foreach{ anno =>
      pToken.addAnnotation(anno.makeAnnotation(token))
    }
    pToken.build()
  }

  def serialize(doc:Document) = {
    assert(doc.hasAnnotation(classOf[Token]), "We need to at least tokenize before we can serialize.")

    val methods:Iterable[AnnotationMethod] = doc.annotators.toSeq.zipWithIndex.flatMap { case((annotation, annotator), index) =>
      AnnotationMethod.fromAnnotation(annotation, annotator, index)
    }
    val proto = protoDocument.setId(doc.name).setText(doc.string).addAllMethod(methods.map(_.toMethod).asJava)
    proto.addAllToken(getTokens(doc, methods.collect{case m:TokenAnnotation => m}).asJava)
    proto.build()
  }

  def deserialize(ser:ProtoDocument):Document = {
    val methodMap = ser.getMethodList.asScala.zipWithIndex.map{ case (pMethod, index) =>
      index -> AnnotationMethod.fromMethod(pMethod)
    }.toMap
    val doc = new Document(ser.getText)
    doc.setName(ser.getId)
    var sentOpt = if(methodMap.values.collectFirst{case s:SentenceBoundaryTokenAnnotation => s}.isDefined) { // if there is Sentence annotation
      Some(new Sentence(doc))
    } else {
      None
    }
    ser.getTokenList.asScala.foreach { pToken =>
      val token = sentOpt match {
        case Some(sent) => new Token(sent, pToken.getStart, pToken.getEnd)
        case None => new Token(doc, pToken.getStart, pToken.getEnd)
      }
      pToken.getAnnotationList.asScala.foreach { pAnno =>
        methodMap(pAnno.getMethodIndex).asInstanceOf[TokenAnnotation].annotateToken(token, pAnno)
        if(methodMap(pAnno.getMethodIndex).isInstanceOf[SentenceBoundaryTokenAnnotation] && pAnno.getVal == SentenceBoundaries.END) { // if this token is at the end of a sentence
          sentOpt = Some(new Sentence(doc))
        }
      }
    }
    doc
  }
}

object ProtoTest {
  def main(args:Array[String]) {
    val docs:Iterable[Document] = Seq()


    docs.map(doc => doc.name -> Proto.serialize(doc))
  }
}
*/
