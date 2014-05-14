package edu.umass.cs.iesl.factorie_protobufs

import cc.factorie.app.nlp.pos.OntonotesForwardPosTagger
import cc.factorie.app.nlp.{Document, DocumentAnnotatorPipeline, MutableDocumentAnnotatorMap}
import java.io.{FileInputStream, FileOutputStream, FileWriter, File}
import scala.io.Source
import edu.umass.cs.iesl.protos._
import cc.factorie.util.FileUtils

/**
 * @author John Sullivan
 */
object FSTest {
  object Pipeline{
    val defaultPipeline = Seq(OntonotesForwardPosTagger)
    val map = new MutableDocumentAnnotatorMap ++= DocumentAnnotatorPipeline.defaultDocumentAnnotationMap
    val pipe = DocumentAnnotatorPipeline(map=map.toMap, prereqs=Nil, defaultPipeline.flatMap(_.postAttrs))
  }

  def getFiles(dir:File):Iterable[File] = dir.listFiles().flatMap{ f =>
    if(f.isDirectory) {
      getFiles(f)
    } else {
      Seq(f)
    }
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

  object Serializer extends AnnotationSuite(Vector(TokenizationAnnotation, NormalizedTokenAnnotation, SentenceAnnotation, POSAnnotation))

  case class FileId(service:String, date:String, id:String) {
    def asId = "%s_%s_%s".format(service, date, id)
    def asFilepath(root:String, extension:String) = new File("%s/%s/%s/%s.%s".format(root, service, date, asId, extension))
    def makeDirectory(root:String) = new File("%s/%s/%s".format(root, service, date)).mkdirs()
  }

  object FileId {
    private val DocRegex = """([\w_]+)_(\d{8})_(.+)""".r

    def apply(file:File):FileId = {
      val idString = file.getName.replaceFirst("[.][^.]+$", "").replaceAll("""\.""","_")
      val DocRegex(source, date, id) = idString
      new FileId(source, date, id)
    }
  }

  def main(args:Array[String]) {
    val docDir = args(0)
    val outputDir = args(1)
    var t1 = System.currentTimeMillis()

    val filenames = new File(docDir).listFiles().filter(_.getName.endsWith(".sgm"))

    val docMap = filenames.map{ file =>
      val id = FileId(file)
      val text = Source.fromFile(file).getLines().mkString
      val doc = new Document(text).setName(id.asId)
      id -> doc
    }
    var t2 = System.currentTimeMillis() - t1
    println("Loaded %s documents in %.4f secs".format(docMap.size, t2/1000.0))
    t1 = System.currentTimeMillis()
    docMap.foreach{case (_, doc) => Pipeline.pipe.process(doc)}
    t2 = System.currentTimeMillis() - t1
    println("Annotated documents in %.4f secs".format(t2/1000.0))
    t1 = System.currentTimeMillis()
    val serMap = docMap.map{ case(id, doc) =>
      id -> Serializer.serialize(doc)
    }
    t2 = System.currentTimeMillis() - t1
    println("Serialized documents in %.4f secs".format(t2/1000.0))
    t1 = System.currentTimeMillis()
    serMap.foreach{ case(id, doc) =>
      id.makeDirectory(outputDir)
      val str = new FileOutputStream(id.asFilepath(outputDir, "pb"))
      doc.writeTo(str)
      str.flush()
      str.close()
    }
    t2 = System.currentTimeMillis() - t1
    println("wrote documents in %.4f secs".format(t2/1000.0))
    t1 = System.currentTimeMillis()
    val sDocs = getFiles(new File(outputDir)).map{ file =>
      readDocument(new FileInputStream(file))
    }
    t2 = System.currentTimeMillis() - t1
    println("Read in documents in %.4f secs".format(t2/1000.0))
    sDocs.map(d => Serializer.deserialize(d))
    t1 = System.currentTimeMillis()
    teardownDir(new File(outputDir))
    t2 = System.currentTimeMillis() - t1
    println("toredown dirs in %.4f secs".format(t2/1000.0))

  }
}
