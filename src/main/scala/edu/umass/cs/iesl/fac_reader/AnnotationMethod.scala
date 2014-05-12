package edu.umass.cs.iesl.fac_reader

import cc.factorie.app.nlp._
import cc.factorie.app.nlp.pos._
import cc.factorie.app.nlp.ner._

import edu.umass.cs.iesl.protos._
import edu.umass.cs.iesl.protos.AnnotationType._
import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * @author John Sullivan
 */
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
