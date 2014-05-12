package edu.umass.cs.iesl.fac_reader

import cc.factorie.app.nlp._
import cc.factorie.app.nlp.pos._
import cc.factorie.app.nlp.ner._

import edu.umass.cs.iesl.protos._
import edu.umass.cs.iesl.protos.AnnotationType._
import scala.collection.mutable
import scala.collection.JavaConverters._

/**
 * @author John Sullivan
 */
trait AnnotationMethod {
  def annotation:String
  def annotationType:AnnotationType
  protected var _annotator:String = null

  private final def method = protoMethod.setAnnotation(annotation).setAnnotator(_annotator).setType(annotationType).build()

  def withAnnotator(annotator:String):AnnotationMethod = {
    _annotator = annotator
    this
  }

  def addMethod(fDoc:DocumentBuilder):DocumentBuilder = fDoc.addMethod(method)

  def serialize(doc:Document, serDoc:DocumentBuilder):DocumentBuilder

  def deserialize(doc:Document, serDoc:ProtoDocument)(annoClass:(Class[_], Class[_])):Document
}

trait TokenLevelAnnotation extends AnnotationMethod {
  def serializeToken(fToken:Token, pToken:TokenBuilder = protoToken):TokenBuilder
  def deserializeToken(pToken:TokenBuilder, fToken:Token):Token

  def deserialize(doc: Document, serDoc:ProtoDocument)(annoClass: (Class[_], Class[_])) = ???
  def serialize(doc: Document, serDoc: DocumentBuilder) = ???
}

object TokenizationAnnotation extends TokenLevelAnnotation {
  val annotation = "cc.factorie.app.nlp.Token"
  val annotationType = AnnotationType.TEXT

  def serializeToken(fToken: Token, pToken: TokenBuilder) = {
    pToken.setStart(fToken.stringStart).setEnd(fToken.stringEnd) //todo check that these are the correct offsets
  }

  def deserializeToken(pToken: TokenBuilder, fToken: Token) = new Token(pToken.getStart, pToken.getEnd)
}

object POSAnnotation extends TokenLevelAnnotation {
  val annotation = "cc.factorie.app.nlp.pos.PennPosTag"
  val annotationType = AnnotationType.TAG

  def serializeToken(fToken:Token, pToken:TokenBuilder) = pToken.addAnnotation(protoAnnotation.setText(fToken.posTag.categoryValue).setType(annotationType).build())
  def deserializeToken(pToken:TokenBuilder, fToken:Token) = {
    new PennPosTag(fToken, pToken.getAnnotation().getText) //todo figure out anno method idx
  }
}

class AnnotationSuite(annotators:IndexedSeq[AnnotationMethod]) {
  private implicit val classMap = mutable.HashMap[String, Class[_]]().withDefault{ className => Class.forName(className) }
  private val nameMap = mutable.HashMap[Class[_], String]().withDefault{ cl => cl.getName }
  private val annotatorMap = annotators.map(a => a.annotation -> a).toMap

  private val (tokenAnnotators, generalAnnotators) = annotators.partition(_.isInstanceOf[TokenLevelAnnotation])

  def serialize(fDoc:Document, sDoc:DocumentBuilder = protoDocument):ProtoDocument = {
    val pTokens = fDoc.tokens.map { fToken =>
      tokenAnnotators.foldLeft(protoToken){case (pToken, anno) =>
        anno.asInstanceOf[TokenLevelAnnotation].serializeToken(fToken, pToken)
      }.build()
    }.asJava
    tokenAnnotators.foreach{_.addMethod(sDoc)}
    sDoc.addAllToken(pTokens)
    generalAnnotators.foreach{ anno =>
      anno.addMethod(sDoc)
      anno.serialize(fDoc, sDoc)
    }
    sDoc.build()
  }

  def deserialize(sDoc:DocumentBuilder, fDoc:Document = new Document()):Document = {
    fDoc.setName(sDoc.getId)
    fDoc.appendString(sDoc.getText)
    sDoc.getMethodList.asScala.foreach { sMethod =>
      annotatorMap.get(sMethod.getAnnotation) match {
        case Some(anno) => anno.deserialize(fDoc, sDoc) //todo fix for deserialize token
        case None => println("WARNING: deserializing %s. No annotation method found for %s".format(sDoc.getId, sMethod.getAnnotation))
      }
    }
    fDoc
  }
  /*
  def fromDocument(doc:Document, serDoc:DocumentBuilder = protoDocument):ProtoDocument = annotators.flatMap { annoMethod =>
    doc.annotatorFor(classMap(annoMethod.annotation)) match {
      case Some(cl) =>
        val annotator = nameMap(cl)
        Some(annoMethod.withAnnotator(annotator))
      case None => println("WARNING: No annotation found for %s in document %s".format(annoMethod.annotation, doc.name)); None
    }
  }.foldLeft(serDoc){case (s, anno) => anno.addSerialization(doc, s)}.build()   //todo This serialization requires n passes per token we want one. have 'token annotation' that scan through with singleton method
  */
  /*
  def fromSerialization(serDoc:ProtoDocument, doc:Document = new Document()):Document = {
    doc.setName(serDoc.getId)
    doc.appendString(serDoc.getText)
    annotators.zipWithIndex.map{ case(annoMethod, idx) =>
      val method = serDoc.getMethod(idx)
      assert(method.getAnnotation == annoMethod.annotation, "ERROR: annotation order should match. Expected %s in position %d, found %s".format(annoMethod.annotation, idx, method.getAnnotation))
      annoMethod.deserialize(idx, doc, serDoc)
    }
    doc
  }
  */
  /*
  def fromDocument(doc:Document, ser:DocumentBuilder = protoDocument):DocumentBuilder = {
    doc.annotators.flatMap { case(annotation, annotator) =>
      annotators.get(annotation.getName) match {
        case Some(a) => Some(a.withAnnotator(annotator))
        case None => println("WARNING: Found unknown annotation (%s) in document (%s)".format(annotation, doc.name)); None
      }
    }.foldLeft(ser){case (s, anno) => anno.addSerialization(doc, s)}
  }
  */
}
/*
object TokenAnnotation extends AnnotationMethod {
  def annotation = "cc.factorie.app.nlp.Token"

  def serialize(doc:Document, serDoc: DocumentBuilder) = {
    serDoc.addMethod(method.setType(AnnotationType.TEXT).build())
    doc.tokens.foreach{ fToken =>
      serDoc.addToken(protoToken.setStart(fToken.stringStart).setEnd(fToken.stringEnd)) // todo check that these are right
    }
    serDoc
  }

  def deserialize(idx:Int, doc: Document, serDoc:ProtoDocument)(implicit classMap:mutable.HashMap[String, Class[_]]) = {
    val method = serDoc.getMethod(idx)
    doc.annotators += classMap(method.getAnnotation) -> classMap(method.getAnnotator)
    serDoc.getTokenList.asScala.foreach { pToken =>
      doc.asSection += new Token(pToken.getStart, pToken.getEnd)
    }
    doc
  }
}

object POSAnnotation extends AnnotationMethod {
  def annotation = "cc.factorie.app.nlp.pos.PennPosTag"

  def serialize(doc:Document, serDoc:DocumentBuilder) = {
    serDoc.addMethod(method.setType(AnnotationType.TAG).build())
    doc.tokens.zipWithIndex.map
  }
}
*/
/*
trait AnnotationMethod {
  def annotation:String
  def annotator:String
  def annotationType:ProtoAnnotationType
  def methodIndex:Int

  import AnnotationMethod._
  def toMethod:ProtoMethod = protoMethod.setAnnotation(annotation).setAnnotator(annotator).setType(annotationType).build()
  def toAnnotationTuple:(Class[_], Class[_]) = classMap(annotation) -> classMap(annotator)
}

abstract class TokenAnnotation(val annotation:String, val annotator:String, val annotationType:ProtoAnnotationType, val methodIndex:Int) extends AnnotationMethod {
  def makeAnnotation(token:Token):ProtoAnnotation
  def annotateToken(token:Token, pAnno:ProtoAnnotation):Unit
}

class PennPosTokenAnnotation(annotation:Class[_], annotator:Class[_], index:Int) extends TokenAnnotation(annotation.getName, annotator.getName, TAG, index) {
  def makeAnnotation(token: Token) = protoAnnotation.setText(token.posTag.categoryValue).build()
  def annotateToken(token: Token, pAnno:ProtoAnnotation) {
    token.attr += new pos.PennPosTag(token, pAnno.getText)
  }
}
class NERTokenAnnotation(annotation:Class[_], annotator:Class[_], index:Int) extends TokenAnnotation(annotation.getName, annotator.getName, TAG, index) {
  def makeAnnotation(token: Token) = protoAnnotation.setText(token.nerTag.categoryValue).build()
  def annotateToken(token:Token, pAnno:ProtoAnnotation) {
    token.attr += new ner.BilouOntonotesNerTag(token, pAnno.getText)
  }
}
class SentenceBoundaryTokenAnnotation (annotation:Class[_], annotator:Class[_], index:Int) extends TokenAnnotation(annotation.getName, annotator.getName, BOUNDARY, index) {
  import SentenceBoundaries._
  def getVal(token:Token) = if(token.isSentenceStart) {
    START
  } else if(token.isSentenceEnd) {
    END
  } else {
    INTERIOR
  }

  def makeAnnotation(token: Token) = protoAnnotation.setVal(getVal(token)).build()
  def annotateToken(token: Token, pAnno:ProtoAnnotation) = Unit
}

object SentenceBoundaries {
  val START = 0
  val INTERIOR = 1
  val END = 2
}


abstract class CompoundAnnotation(val annotation:String, val annotator:String, val annotationType:ProtoAnnotationType) extends AnnotationMethod {

}

/*
case class AnnotationMethod(annotation:String, annotator:String, annotationType:ProtoAnnotationType) {
}
*/

object AnnotationMethod {
  private val classMap = mutable.HashMap[String, Class[_]]().withDefault(Class.forName)

  def fromMethod(m:ProtoMethod):AnnotationMethod = ???

  private val unknownAnnotations = mutable.HashMap[(String, String), Int]().withDefaultValue(0)

  def fromAnnotation(annotation:Class[_], annotator:Class[_], index:Int)(implicit tag:ClassTag[PennPosTag], ner:ClassTag[BilouOntonotesNerTag], t:ClassTag[Token], s:ClassTag[Sentence]):Option[AnnotationMethod] = annotation match {
    case _:Class[PennPosTag] => Some(new PennPosTokenAnnotation(annotation, annotator, index))
    case _:Class[BilouOntonotesNerTag] => Some(new NERTokenAnnotation(annotation, annotator, index))
    case _:Class[Sentence] => Some(new SentenceBoundaryTokenAnnotation(annotation, annotator, index))
    case _:Class[Token] => None

    case _ => {
      unknownAnnotations.put(annotation.getName -> annotator.getName, unknownAnnotations(annotation.getName -> annotator.getName) + 1)
      None
    }
  }
  /*
  val fromAnnotationTuple:PartialFunction[(Class[_], Class[_]), AnnotationMethod] = {
    case tup:(Class[nlp.pos.PennPosTag], _) => new TokenAnnotation(tup._1.getName, tup._2.getName, TAG) {
      override def makeAnnotation(token: Token) = protoAnnotation.setText(token.posTag.categoryValue).build()
    }
    case tup:(Class[nlp.ner.BilouOntonotesNerTag], _) => new TokenAnnotation(tup._1.getName, tup._2.getName, TAG) {
      override def makeAnnotation(token: Token) = protoAnnotation.setText(token.nerTag.categoryValue).build()
    }
  }
  */
  //def fromAnnotationTuple(annotation:Class[_], annotator:Class[_]) = AnnotationMethod(annotation.getName, annotator.getName, OTHER)
}
*/
