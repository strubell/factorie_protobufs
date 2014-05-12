package edu.umass.cs.iesl.fac_reader

import com.mongodb._
import com.mongodb.gridfs._

import edu.umass.cs.iesl.protos._
import cc.factorie.app.nlp.{Document, Token}

import scala.collection.JavaConverters._
import java.io.File
import scala.io.Source
import cc.factorie.app.nlp.segment.DeterministicTokenizer

/**
 * @author John Sullivan
 */
object SpeedTest {

  def pTokens(doc:Document):Iterable[ProtoToken] = doc.tokens.map{ token =>
    protoToken.setStart(token.stringStart).setEnd(token.stringEnd).build()
  }

  def serialize(doc:Document):ProtoDocument = protoDocument.setText(doc.string).setId(doc.name).addAllToken(pTokens(doc).asJava).build()


  def deserialize(pDoc:ProtoDocument):Document = {
    val doc = new Document(pDoc.getText).setName(pDoc.getId)
    pDoc.getTokenList.asScala.foreach { pToken =>
      new Token(doc, pToken.getStart, pToken.getEnd)
    }
    doc
  }


  def getDBColl() = {
    val dbClient = new MongoClient("localhost", 27017)
    val db = dbClient.getDB("serialization-test")
    val coll = db.getCollection("docs")
    db -> coll
  }

  def teardownDB(db:DB) {
    db.dropDatabase()
  }

  def timeDb(docs:Iterable[ProtoDocument]):(Double)

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
    val tokenizer = new DeterministicTokenizer(false, true, false, false, false)
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

}
