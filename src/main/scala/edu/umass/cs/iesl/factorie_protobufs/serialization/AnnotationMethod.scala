package edu.umass.cs.iesl.factorie_protobufs.serialization

import cc.factorie.app.nlp._
import cc.factorie.app.nlp.pos._

import edu.umass.cs.iesl.protos._
import scala.collection.JavaConverters._
import scala.io.Source
import java.io.{BufferedWriter, FileWriter, FileInputStream, FileOutputStream}

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

  def deserializeCompound(sCompound:ProtoCompoundGroup, fDoc:Document):Document
  def serializeCompound(fDoc:Document):ProtoCompoundGroup
}

trait TokenLevelAnnotation extends AnnotationMethod {
  def serializeToken(fToken:Token, pToken:TokenBuilder = protoToken):TokenBuilder
  def deserializeToken(pToken:ProtoToken, fToken:Token):Token
  protected final def indexedAnnotation = protoAnnotation.setType(annotationType).setMethodIndex(_methodIndex)


  def deserializeAnnotatation(sAnno:ProtoAnnotation, fToken:Token):Token

  def deserialize(doc: Document, serDoc:ProtoDocument)(annoClass: (Class[_], Class[_])) = ???
  def serialize(doc: Document, serDoc: DocumentBuilder) = ???
}

object TokenizationAnnotation extends TokenLevelAnnotation {
  val annotation = "cc.factorie.app.nlp.Token"
  val annotationType = AnnotationType.TEXT

  def serializeToken(fToken: Token, pToken: TokenBuilder) = {
    pToken.setStart(fToken.stringStart).setEnd(fToken.stringEnd).addAnnotation(indexedAnnotation.build()) //todo check that these are the correct offsets
  }

  def deserializeToken(pToken: ProtoToken, fToken: Token) = new Token(pToken.getStart, pToken.getEnd)
}

object POSAnnotation extends TokenLevelAnnotation {
  val annotation = "cc.factorie.app.nlp.pos.PennPosTag"
  val annotationType = AnnotationType.TAG

  def serializeToken(fToken:Token, pToken:TokenBuilder) = pToken.addAnnotation(indexedAnnotation.setText(fToken.posTag.categoryValue).build())
  def deserializeToken(pToken:ProtoToken, fToken:Token) = {
    fToken.attr += new PennPosTag(fToken, pToken.getAnnotation(_methodIndex).getText)
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
      sSentences.addCompound(protoCompound.addSlot(boundaries).build())
    }
    serDoc.addMethod(methodProto).addCompound(sSentences.build())
  }
}

object SerDeTest {
  def main(args:Array[String]) {
    val docPath = args(0)

    val doc = new Document(Source.fromFile(docPath).getLines().mkString).setName("testId")
    println("Loaded document")

    Pipeline.pipe.process(doc)
    println("Annotated document")

    val annos = new AnnotationSuite(Vector(TokenizationAnnotation, PlainNormalizedTokenAnnotation, SentenceAnnotation, POSAnnotation))

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