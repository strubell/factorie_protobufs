package edu.umass.cs.iesl.factorie_protobufs

import cc.factorie.app.nlp.pos.OntonotesForwardPosTagger
import cc.factorie.app.nlp.{Document, DocumentAnnotatorPipeline, MutableDocumentAnnotatorMap}
import java.io.File
import scala.io.Source
import edu.umass.cs.iesl.factorie_protobufs.io.FileId

/**
 * @author John Sullivan
 */
object FSTest {
  object Pipeline{
    val defaultPipeline = Seq(OntonotesForwardPosTagger)
    val map = new MutableDocumentAnnotatorMap ++= DocumentAnnotatorPipeline.defaultDocumentAnnotationMap
    val pipe = DocumentAnnotatorPipeline(map=map.toMap, prereqs=Nil, defaultPipeline.flatMap(_.postAttrs))
  }

  def teardownDir(dir:File) {
    dir.listFiles().foreach { f =>
      if(f.isDirectory) {
        teardownDir(f)
        f.delete()
      } else {
        f.delete()
      }
    }
  }

  def main(args:Array[String]) {
    val docDir = args(0)
    val outputDir = args(1)
    var t1 = System.currentTimeMillis()

    val filenames = new File(docDir).listFiles().filter(_.getName.endsWith(".sgm"))

    val docs = filenames.map{ file =>
      val id = FileId(file)
      val text = Source.fromFile(file).getLines().mkString
      val doc = new Document(text).setName(id.asId)
      doc
    }
    var t2 = System.currentTimeMillis() - t1
    println("Loaded %s documents in %.4f secs".format(docs.size, t2/1000.0))
    t1 = System.currentTimeMillis()
    docs.foreach{doc => Pipeline.pipe.process(doc)}
    t2 = System.currentTimeMillis() - t1
    println("Annotated documents in %.4f secs".format(t2/1000.0))
    t1 = System.currentTimeMillis()
    val serDocs = docs.map(_.serialize)
    t2 = System.currentTimeMillis() - t1
    println("Serialized documents in %.4f secs".format(t2/1000.0))
    t1 = System.currentTimeMillis()
    serDocs.foreach{_.writeStructuredTo(outputDir)}
    t2 = System.currentTimeMillis() - t1
    println("wrote documents in %.4f secs".format(t2/1000.0))
    t1 = System.currentTimeMillis()
    val sDocs = ProtoDocument.readStructuredFrom(outputDir)
    t2 = System.currentTimeMillis() - t1
    println("Read in documents in %.4f secs".format(t2/1000.0))
    sDocs.map(_.deserialize)
    t1 = System.currentTimeMillis()
    teardownDir(new File(outputDir))
    t2 = System.currentTimeMillis() - t1
    println("toredown dirs in %.4f secs".format(t2/1000.0))

  }
}
