package edu.umass.cs.iesl.factorie_protobufs.io

import java.io.File

/**
 * @author John Sullivan
 */
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

  def apply(idString:String):FileId = {
    val DocRegex(source, date, id) = idString
    new FileId(source, date, id)
  }
}