package edu.umass.cs.iesl.factorie_protobufs

import cc.factorie.app.nlp._
import cc.factorie.app.nlp.pos._

import edu.umass.cs.iesl.protos._
import scala.collection.mutable
import scala.collection.JavaConverters._
import cc.factorie.app.nlp.ner.BilouConllNerTag
import scala.io.Source
import java.io.{BufferedWriter, FileWriter, FileInputStream, FileOutputStream}
import cc.factorie.app.nlp.segment.PlainNormalizedTokenString

/**
  * @author John Sullivan
 */
trait AnnotationMethod {
  def annotation:String
  def annotationType:AnnotationType
  var annotator:String = null
  protected var _methodIndex = null.asInstanceOf[Int]

  protected final def methodProto = protoMethod.setAnnotation(annotation).setAnnotator(annotator).setType(annotationType).build()

  def withMethodIndex(methodIndex:Int):AnnotationMethod = {
    _methodIndex = methodIndex
    this
  }
  def withAnnotator(_annotator:String):AnnotationMethod = {
    annotator = _annotator
    this
  }

  def addMethod(fDoc:DocumentBuilder):DocumentBuilder = fDoc.addMethod(methodProto)

  def serialize(doc:Document, serDoc:DocumentBuilder):DocumentBuilder

  def deserialize(doc:Document, serDoc:ProtoDocument)(annoClass:(Class[_], Class[_])):Document
}

trait TokenLevelAnnotation extends AnnotationMethod {
  def serializeToken(fToken:Token, pToken:TokenBuilder = protoToken):TokenBuilder
  def deserializeToken(pToken:ProtoToken, fToken:Token):Token
  protected final def annotationProto = protoAnnotation.setType(annotationType).setMethodIndex(_methodIndex)


  def deserialize(doc: Document, serDoc:ProtoDocument)(annoClass: (Class[_], Class[_])) = ???
  def serialize(doc: Document, serDoc: DocumentBuilder) = ???
}

object TokenizationAnnotation extends TokenLevelAnnotation {
  val annotation = "cc.factorie.app.nlp.Token"
  val annotationType = AnnotationType.TEXT

  def serializeToken(fToken: Token, pToken: TokenBuilder) = {
    pToken.setStart(fToken.stringStart).setEnd(fToken.stringEnd).addAnnotation(annotationProto.build()) //todo check that these are the correct offsets
  }

  def deserializeToken(pToken: ProtoToken, fToken: Token) = new Token(pToken.getStart, pToken.getEnd)
}

object POSAnnotation extends TokenLevelAnnotation {
  val annotation = "cc.factorie.app.nlp.pos.PennPosTag"
  val annotationType = AnnotationType.TAG

  def serializeToken(fToken:Token, pToken:TokenBuilder) = pToken.addAnnotation(annotationProto.setText(fToken.posTag.categoryValue).build())
  def deserializeToken(pToken:ProtoToken, fToken:Token) = {
    fToken.attr += new PennPosTag(fToken, pToken.getAnnotation(_methodIndex).getText)
    fToken
  }
}

object NormalizedTokenAnnotation extends TokenLevelAnnotation {
  val annotation = "cc.factorie.app.nlp.segment.PlainNormalizedTokenString"
  val annotationType = AnnotationType.TAG

  def serializeToken(fToken:Token, pToken:TokenBuilder) = {
    val pAnno = protoAnnotation.setType(annotationType)
    if(fToken.attr.contains[PlainNormalizedTokenString]) {
      pAnno.setText(fToken.attr[PlainNormalizedTokenString].value)
    }
    pToken.addAnnotation(pAnno.build())
  }
  def deserializeToken(pToken:ProtoToken, fToken:Token) = {
    fToken.attr += new PlainNormalizedTokenString(fToken, pToken.getAnnotation(_methodIndex).getText)
    fToken
  }
}

object BILOUNERAnnotation extends TokenLevelAnnotation {
  val annotation = "cc.factorie.app.nlp.ner.BilouOntonotesNerTag"
  val annotationType = AnnotationType.TAG

  def serializeToken(fToken:Token, pToken:TokenBuilder) = pToken.addAnnotation(protoAnnotation.setText(fToken.nerTag.categoryValue).setType(annotationType).build())
  def deserializeToken(pToken:ProtoToken, fToken:Token) = {
    fToken.attr += new BilouConllNerTag(fToken, pToken.getAnnotation(_methodIndex).getText)
    fToken
  }
}

object SentenceAnnotation extends AnnotationMethod {
  val annotation = "cc.factorie.app.nlp.Sentence"
  val annotationType = AnnotationType.BOUNDARY

  def deserialize(doc: Document, serDoc: ProtoDocument)(annoClass: (Class[_], Class[_])) = {
    val sentenceCompound = serDoc.getCompound(_methodIndex)
    assert(sentenceCompound.getCompoundCount == 1)
    sentenceCompound.getCompound(0).getSlotList.asScala.foreach { sSentence =>
      new Sentence(doc.asSection, sSentence.getStartToken, sSentence.getEndToken)
    }
    doc
  }

  def serialize(doc: Document, serDoc: DocumentBuilder) = {
    val sSentences = protoCompoundGroup.setType(annotationType).setMethodIndex(_methodIndex)
    doc.sentences.foreach { fSentence =>
      val boundaries = protoSlot.setStartToken(fSentence.start).setEndToken(fSentence.end).build()
      sSentences.addCompound(protoCompund.addSlot(boundaries).build())
    }
    serDoc.addMethod(methodProto).addCompound(sSentences.build())
  }
}

class AnnotationSuite(val annotators:IndexedSeq[AnnotationMethod]) {
  private implicit val classMap = mutable.HashMap[String, Class[_]]().withDefault{ className => Class.forName(className) }
  private val nameMap = mutable.HashMap[Class[_], String]().withDefault{ cl => cl.getName }
  private val annotatorMap = annotators.map(a => a.annotation -> a).toMap

  private val (tokenAnnotators, generalAnnotators) = annotators.partition(_.isInstanceOf[TokenLevelAnnotation])

  def serialize(fDoc:Document, sDoc:DocumentBuilder = protoDocument):ProtoDocument = {
    sDoc.setId(fDoc.name)
    sDoc.setText(fDoc.string)

    fDoc.annotators.foreach { case(annotationClass, annotatorClass) =>
      val annotation = nameMap(annotationClass); val annotator = nameMap(annotatorClass)
      annotatorMap.get(annotation) match {
        case Some(anno) => anno.withAnnotator(annotator)
        case None => println("WARNING: Document %s had annotation %s with no corresponding serializer".format(fDoc.name, annotation))
      }
    }
    annotatorMap.foreach { case (annotation, _) =>
      fDoc.annotators.get(classMap(annotation)) match {
        case Some(_) => Unit
        case None => println("WARNING: Document %s does not contain expected annotation %s".format(fDoc.name, annotation))
      }
    }

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

  def deserialize(sDoc:ProtoDocument, fDoc:Document = new Document()):Document = {
    fDoc.setName(sDoc.getId)
    fDoc.appendString(sDoc.getText)
    annotators.foreach { annoMethod =>
      fDoc.annotators += classMap(annoMethod.annotation) -> classMap(annoMethod.annotator)
    }
    sDoc.getMethodList.asScala.zipWithIndex.foreach { case(sMethod, idx) =>
      annotatorMap.get(sMethod.getAnnotation) match {
        case Some(anno) if anno.isInstanceOf[TokenLevelAnnotation] => anno.asInstanceOf[TokenLevelAnnotation].withMethodIndex(idx)
        case Some(_) => Unit
        case None => println("WARNING: deserializing %s. No annotation method found for %s".format(sDoc.getId, sMethod.getAnnotation))
      }
    }
    sDoc.getTokenList.asScala.foreach { sToken =>
      fDoc.asSection += tokenAnnotators.foldLeft(null.asInstanceOf[Token]) { case(fToken, anno) =>
        anno.asInstanceOf[TokenLevelAnnotation].deserializeToken(sToken, fToken)
      }
    }
    fDoc
  }
}

object SerDeTest {
  def main(args:Array[String]) {
    val docPath = args(0)

    val doc = new Document(Source.fromFile(docPath).getLines().mkString).setName("testId")
    println("Loaded document")

    Pipeline.pipe.process(doc)
    println("Annotated document")

    val annos = new AnnotationSuite(Vector(TokenizationAnnotation, NormalizedTokenAnnotation, SentenceAnnotation, POSAnnotation))

    val sDoc = annos.serialize(doc)
    println("serialized document")
    val wrt = new FileOutputStream("test.pb")
    sDoc.writeTo(wrt)
    wrt.flush()
    wrt.close()
    println("wrote document")
    val wrt2 = new BufferedWriter(new FileWriter("protostring"))

    wrt2.write(sDoc.toString)
    wrt2.flush()
    wrt2.close()

    val sDoc2 = readDocument(new FileInputStream("test.pb"))
    println("Read back serialzed doc")
    val doc2 = annos.deserialize(sDoc2)
    println("serialized doc")
    assert(annos.annotators.forall { anno => doc2.hasAnnotation(Class.forName(anno.annotation)) })
  }

  object Pipeline{
    val defaultPipeline = Seq(OntonotesForwardPosTagger)
    val map = new MutableDocumentAnnotatorMap ++= DocumentAnnotatorPipeline.defaultDocumentAnnotationMap
    val pipe = DocumentAnnotatorPipeline(map=map.toMap, prereqs=Nil, defaultPipeline.flatMap(_.postAttrs))
  }
}