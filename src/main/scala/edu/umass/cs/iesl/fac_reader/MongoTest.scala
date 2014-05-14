package edu.umass.cs.iesl.fac_reader

import com.mongodb.{DB, MongoClient}
import java.io.File
import scala.io.Source
import cc.factorie.app.nlp.{DocumentAnnotatorPipeline, MutableDocumentAnnotatorMap, Document}
import com.mongodb.gridfs.GridFS
import cc.factorie.app.nlp.pos.OntonotesForwardPosTagger
import scala.collection.JavaConverters._
import edu.umass.cs.iesl.protos._

/**
 * @author John Sullivan
 */
object MongoTest {
  def getDBColl() = {
    val dbClient = new MongoClient("localhost", 27017)
    val db = dbClient.getDB("serialization-test")
    val coll = db.getCollection("docs")
    db -> coll
  }

  def teardownDB(db:DB) {
    db.dropDatabase()
  }

  object Pipeline{
    val defaultPipeline = Seq(OntonotesForwardPosTagger)
    val map = new MutableDocumentAnnotatorMap ++= DocumentAnnotatorPipeline.defaultDocumentAnnotationMap
    val pipe = DocumentAnnotatorPipeline(map=map.toMap, prereqs=Nil, defaultPipeline.flatMap(_.postAttrs))
  }

  object Serializer extends AnnotationSuite(Vector(TokenizationAnnotation, NormalizedTokenAnnotation, SentenceAnnotation, POSAnnotation))

  //def timeDb(docs:Iterable[ProtoDocument]):(Double)

  def main(args:Array[String]) {
    val docDir = args(0)
    var t1 = System.currentTimeMillis()

    val fileNames = new File(docDir).listFiles().filter(_.getName.endsWith(".sgm"))

    val docs = fileNames.map{ fileName =>
      val id = fileName.getName.replaceFirst("[.][^.]+$", "").replaceAll("""\.""","_")
      val text = Source.fromFile(fileName).getLines().mkString
      new Document(text).setName(id)
    }.toSeq

    var t2 = System.currentTimeMillis() - t1
    println("Loaded %d documents in %.4f secs".format(docs.size, t2/1000.0))
    t1 = System.currentTimeMillis()
    docs.foreach(Pipeline.pipe.process)
    t2 = System.currentTimeMillis() - t1
    println("Annotated in %.4f secs".format(t2/1000.0))

    t1 = System.currentTimeMillis()
    val serialized = docs.map(doc => doc.name -> Serializer.serialize(doc))
    //val serialized = docs.map(doc => doc.name -> serialize(doc))
    t2 = System.currentTimeMillis() - t1
    println("Serialized in %.4f secs".format(t2/1000.0))

    t1 = System.currentTimeMillis()
    val (db, coll) = getDBColl()
    t2 = System.currentTimeMillis() - t1
    println("DB setup time: %.4f secs".format(t2/1000.0))
    t1 = System.currentTimeMillis()
    val dbos = serialized.map{case(id, s) =>
      val gfs = new GridFS(db, "docs")
      val serializedFile = gfs.createFile(s.toByteArray)
      serializedFile.setId(id)
      serializedFile.save()
      //new BasicDBObject(id, s).asInstanceOf[DBObject]
    }
    //coll.insert(dbos.asJava)
    t2 = System.currentTimeMillis() - t1
    println("DB Insert time: %.4f secs".format(t2/1000.0))
    t1 = System.currentTimeMillis()
    val gfs = new GridFS(db, "docs")
    val sDocs2 = gfs.getFileList.iterator().asScala.map{ gfsFile =>
      readDocument(gfs.findOne(gfsFile).getInputStream)
    }.toSeq
    /*
    val sDocs2 = gfs.find("*").asScala.map{ gfsFile =>
      readDocument(gfsFile.getInputStream)
    }.toSeq
    */
    t2 = System.currentTimeMillis() - t1
    println("Read in %d documents in %.4f secs".format(sDocs2.size, t2/1000.0))

    t1 = System.currentTimeMillis()
    teardownDB(db)
    t2 = System.currentTimeMillis() - t1
    println("Toredown DB in %.4f secs".format(t2/1000.0))
  }
  /*
  def main(args:Array[String]) {
    val docDir = args(0)
    var t1 = System.currentTimeMillis()

    val fileNames = new File(docDir).listFiles().filter(_.getName.endsWith(".sgm"))

    val docs = fileNames.map{filename =>
      val id = filename.getName.replaceFirst("[.][^.]+$", "").replaceAll("""\.""","_")
      val text = Source.fromFile(filename).mkString
      new Document(text).setName(id)
    }.toSeq
    var t2 = System.currentTimeMillis() - t1
    println("Loaded %d documents in %.4f secs".format(docs.size, t2/1000.0))
    t1 = System.currentTimeMillis()
    //val tokenizer = new DeterministicTokenizer(false, true, false, false, false)
    docs.foreach(tokenizer.process)
    t2 = System.currentTimeMillis() - t1
    println("Tokenized in %.4f secs".format(t2/1000.0))

    t1 = System.currentTimeMillis()
    val serialized = docs.map(doc => doc.name -> serialize(doc))
    t2 = System.currentTimeMillis() - t1
    println("Serialized in %.4f secs".format(t2/1000.0))

    t1 = System.currentTimeMillis()
    val (db, coll) = getDBColl()
    t2 = System.currentTimeMillis() - t1
    println("DB setup time: %.4f secs".format(t2/1000.0))
    t1 = System.currentTimeMillis()
    val dbos = serialized.map{case(id, s) =>
      val gfs = new GridFS(db, "docs")
      val serializedFile = gfs.createFile(s.toByteArray)
      serializedFile.setId(id)
      serializedFile.save()
      //new BasicDBObject(id, s).asInstanceOf[DBObject]
    }
    //coll.insert(dbos.asJava)
    t2 = System.currentTimeMillis() - t1
    println("DB Insert time: %.4f secs".format(t2/1000.0))

    t1 = System.currentTimeMillis()
    teardownDB(db)
    t2 = System.currentTimeMillis() - t1
    println("Toredown DB in %.4f secs".format(t2/1000.0))

  }
  */
}
