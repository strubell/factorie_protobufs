package edu.umass.cs.iesl

import cc.factorie.app.nlp.Document
import edu.umass.cs.iesl.protos._
import edu.umass.cs.iesl.factorie_protobufs.serialization._
import edu.umass.cs.iesl.factorie_protobufs.io.FileId
import java.io.{FileInputStream, File, FileOutputStream}

/**
 * @author John Sullivan
 */
package object factorie_protobufs {
  /*
  var defaultAnnotationSuite = new AnnotationSuite(
    Vector(
      TokenAnnotation,
      PlainNormalizedTokenAnnotation,
      OntonotesNormalizedTokenAnnotation,
      SimplifyDigitsLemmaAnnotation,
      CollapseDigitsLemmaAnnotation,
      LowercaseLemmaAnnotation,
      PorterLemmaAnnotation,
      WordnetLemmaAnnotation,
      GeneralLemmaAnnotation,
      SentenceAnnotation,
      SectionAnnotation,
      POSAnnotation,
      BILOUConllNERAnnotation,
      BILOUOntonotesNERAnnotation,
      BIOConllNERAnnotation,
      BIOOntonotesNERAnnotation,
      CorefAnnotation))
      */
  var defaultAnnotationSuite = new AnnotationSuite(
    TokenAnnotation,
    Vector(PlainNormalizedTokenAnnotation,
      OntonotesNormalizedTokenAnnotation,
      SimplifyDigitsLemmaAnnotation,
      CollapseDigitsLemmaAnnotation,
      LowercaseLemmaAnnotation,
      PorterLemmaAnnotation,
      WordnetLemmaAnnotation,
      GeneralLemmaAnnotation,
      POSAnnotation,
      BILOUConllNERAnnotation,
      BILOUOntonotesNERAnnotation,
      BIOConllNERAnnotation,
      BIOOntonotesNERAnnotation),
    Vector(SentenceAnnotation,
      SectionAnnotation,
      CorefAnnotation))

  implicit class DocumentSerialization(doc:Document) {
    def serialize:ProtoDocument = defaultAnnotationSuite.serialize(doc)
  }

  implicit class ProtoDocumentDeserialization(pDoc:ProtoDocument) {
    def deserialize:Document = defaultAnnotationSuite.deserialize(pDoc)

    def writeStructuredTo(dir:String) {
      val id = FileId(pDoc.getId)
      id.makeDirectory(dir)
      val os = new FileOutputStream(id.asFilepath(dir, "pb"))
      pDoc.writeTo(os)
      os.flush()
      os.close()
    }
  }

  object ProtoDocument {
    private def getFiles(dir:File):Iterable[File] = dir.listFiles().flatMap{ f =>
      if(f.isDirectory) {
        getFiles(f)
      } else {
        Seq(f)
      }
    }

    def readById(dir:String, idString:String):Option[ProtoDocument] = if(FileId(idString).asFilepath(dir, "pb").exists()) {
      Some(readDocument(new FileInputStream(FileId(idString).asFilepath(dir, "pb"))))
    } else {
      None
    }

    def readStructuredFrom(dir:String):Iterable[ProtoDocument] = getFiles(new File(dir)).map{ file => readDocument(new FileInputStream(file)) }
  }
}
