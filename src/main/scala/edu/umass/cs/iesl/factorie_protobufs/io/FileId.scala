package edu.umass.cs.iesl.factorie_protobufs.io

import java.io.File

/**
 * @author John Sullivan
 */
//case class FileId(service:String, date:String, id:String) {
//  def asId = "%s_%s_%s".format(service, date, id)
//  def asFilepath(root:String, extension:String) = new File("%s/%s/%s/%s.%s".format(root, service, date, asId, extension))
//  def makeDirectory(root:String) = new File("%s/%s/%s".format(root, service, date)).mkdirs()
//}

case class FileId(service:String, id1:String, id2:String) {
  def asId = "%s-%s-%s".format(service, id1, id2)
  def asFilepath(root:String, extension:String) = new File("%s/%s/%s/%s/%s.%s".format(root, service, id1, id2, asId, extension))
  def makeDirectory(root:String) = new File("%s/%s/%s/%s".format(root, service, id1, id2)).mkdirs()
}

object FileId {
//  private val DocRegex = """([\w_]+)_(\d{8})_(.+)""".r
  private val DocRegex = """(\D+)[-_](\d+-?\d+)[-.](\d+)""".r

  // e.g. eng-NG-31-109501-8131900, bolt-eng-DF-170-181103-15978491
//  private val DocRegex1 = """\s+-(\d+)-(\d{4})(\d{2})-(\d+)""".r

  // e.g. AFP_ENG_20091101.0001
//  private val DocRegex2 = """\s+_(\d{4})(\d{2})(\d{2})\.(\d+)""".r

  def apply(file:File):FileId = {
    val idString = file.getName//.replaceFirst("[.][^.]+$", "").replaceAll("""\.""","_")
    val DocRegex(source, id1, id2) = idString
    new FileId(source, id1.replaceFirst("-", ""), id2)
  }

  def apply(idString:String):FileId = {
    val DocRegex(source, id1, id2) = idString
    new FileId(source, id1.replaceFirst("-", ""), id2)
  }
}