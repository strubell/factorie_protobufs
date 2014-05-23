package edu.umass.cs.iesl.factorie_protobufs.serialization

import cc.factorie.app.nlp._
import cc.factorie.app.nlp.pos._

import edu.umass.cs.iesl.protos._
import scala.collection.mutable
import scala.collection.JavaConverters._
import cc.factorie.app.nlp.ner._
import scala.io.Source
import java.io.{BufferedWriter, FileWriter, FileInputStream, FileOutputStream}
import cc.factorie.app.nlp.segment.PlainNormalizedTokenString
import cc.factorie.app.nlp.lemma._
import scala.reflect.ClassTag

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
  protected final def indexedAnnotation = protoAnnotation.setType(annotationType).setMethodIndex(_methodIndex)


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

class GenericLemmaAnnotation[Lemma <: TokenLemma](constructor:((Token, String) => Lemma))(implicit m:ClassTag[Lemma]) extends TokenLevelAnnotation {
  val annotation:String = m.runtimeClass.getName
  val annotationType = AnnotationType.TEXT

  def serializeToken(fToken:Token, pToken:TokenBuilder) = {
    val pAnno = indexedAnnotation.setType(annotationType)
    if(fToken.attr.contains[Lemma]) {
      pAnno.setText(fToken.attr[Lemma].value)
    }
    pToken.addAnnotation(pAnno.build())
  }

  def deserializeToken(pToken:ProtoToken, fToken:Token) = {
    fToken.attr += constructor(fToken, pToken.getAnnotation(_methodIndex).getText)
    fToken
  }
}

object SimplifyDigitsLemmaAnnotation extends GenericLemmaAnnotation({(t:Token, s:String) => new SimplifyDigitsTokenLemma(t, s)})
object CollapseDigitsLemmaAnnotation extends GenericLemmaAnnotation({(t:Token, s:String) => new CollapseDigitsTokenLemma(t, s)})
object LowercaseLemmaAnnotation extends GenericLemmaAnnotation({(t:Token, s:String) => new LowercaseTokenLemma(t, s)})
object PorterLemmaAnnotation extends GenericLemmaAnnotation({(t:Token, s:String) => new PorterTokenLemma(t, s)})
object WordnetLemmaAnnotation extends GenericLemmaAnnotation({(t:Token, s:String) => new WordNetTokenLemma(t, s)})
object GeneralLemmaAnnotation extends GenericLemmaAnnotation({(t:Token, s:String) => new TokenLemma(t, s)})

object POSAnnotation extends TokenLevelAnnotation {
  val annotation = "cc.factorie.app.nlp.pos.PennPosTag"
  val annotationType = AnnotationType.TAG

  def serializeToken(fToken:Token, pToken:TokenBuilder) = pToken.addAnnotation(indexedAnnotation.setText(fToken.posTag.categoryValue).build())
  def deserializeToken(pToken:ProtoToken, fToken:Token) = {
    fToken.attr += new PennPosTag(fToken, pToken.getAnnotation(_methodIndex).getText)
    fToken
  }
}

object NormalizedTokenAnnotation extends TokenLevelAnnotation {
  val annotation = "cc.factorie.app.nlp.segment.PlainNormalizedTokenString"
  val annotationType = AnnotationType.TEXT

  def serializeToken(fToken:Token, pToken:TokenBuilder) = {
    val pAnno = indexedAnnotation.setType(annotationType)
    if(fToken.attr.contains[PlainNormalizedTokenString]) {
      pAnno.setText(fToken.attr[PlainNormalizedTokenString].value)
    }
    pToken.addAnnotation(pAnno.build())
    pToken
  }
  def deserializeToken(pToken:ProtoToken, fToken:Token) = {
    fToken.attr += new PlainNormalizedTokenString(fToken, pToken.getAnnotation(_methodIndex).getText)
    fToken
  }
}

class GenericNERAnnotation[NER <: NerTag](constructor:((Token, String) => NER))(implicit m:ClassTag[NER]) extends TokenLevelAnnotation {
  val annotation = m.runtimeClass.getName
  val annotationType = AnnotationType.TAG

  def serializeToken(fToken:Token, pToken:TokenBuilder) = {
    val pAnno = indexedAnnotation.setType(annotationType)
    if(fToken.attr.contains[NER]) {
      pAnno.setText(fToken.attr[NER].value)
    }
    pToken.addAnnotation(pAnno.build())
  }

  def deserializeToken(pToken:ProtoToken, fToken:Token) = {
    fToken.attr += constructor(fToken, pToken.getAnnotation(_methodIndex).getText)
    fToken
  }
}

object BILOUConllNERAnnotation extends GenericNERAnnotation({(t, s) => new BilouConllNerTag(t, s)})
object BILOUOntonotesNERAnnotation extends GenericNERAnnotation({(t, s) => new BilouOntonotesNerTag(t, s)})
object BIOConllNERAnnotation extends GenericNERAnnotation({(t, s) => new BioConllNerTag(t, s)})
object BIOOntonotesNERAnnotation extends GenericNERAnnotation({(t, s) => new BioOntonotesNerTag(t, s)})

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

    var idx = 0
    fDoc.annotators.foreach { case(annotationClass, annotatorClass) =>
      val annotation = nameMap(annotationClass); val annotator = nameMap(annotatorClass)
      annotatorMap.get(annotation) match {
        case Some(anno) =>
          val a = anno.withAnnotator(annotator).withMethodIndex(idx)
          idx += 1
          a
        case None => println("WARNING: Document %s had annotation %s with no corresponding serializer".format(fDoc.name, annotation))
      }
    }
    val presentAnnotations = annotatorMap.flatMap { case (annotation, _) =>
      fDoc.annotators.get(classMap(annotation)) match {
        case Some(_) => Some(annotation)
        case None => None
      }
    }.toSet

    val pTokens = fDoc.tokens.map { fToken =>
      tokenAnnotators.filter(a => presentAnnotations.contains(a.annotation)).foldLeft(protoToken){case (pToken, anno) =>
        anno.asInstanceOf[TokenLevelAnnotation].serializeToken(fToken, pToken)
      }.build()
    }.asJava
    tokenAnnotators.filter(a => presentAnnotations.contains(a.annotation)).foreach{_.addMethod(sDoc)}
    sDoc.addAllToken(pTokens)
    generalAnnotators.filter(a => presentAnnotations.contains(a.annotation)).foreach{ anno =>
      anno.addMethod(sDoc)
      anno.serialize(fDoc, sDoc)
    }
    sDoc.build()
  }

  def deserialize(sDoc:ProtoDocument, fDoc:Document = new Document()):Document = {
    fDoc.setName(sDoc.getId)
    fDoc.appendString(sDoc.getText)
    val presentAnnotators = sDoc.getMethodList.asScala.flatMap { method =>
      annotatorMap.get(method.getAnnotation)
    }
    val presentTokenAnnotators = presentAnnotators.collect{case a:TokenLevelAnnotation => a}
    presentAnnotators.foreach { annoMethod =>
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
      fDoc.asSection += presentTokenAnnotators.foldLeft(null.asInstanceOf[Token]) { case(fToken, anno) =>
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