// spark-shell --driver-memory 6G -i working_with_xml.scala
import scala.xml.XML
val xml = XML.loadFile("data/Posts.xml")
val texts = (xml \ "row").map{_.attribute("Body")}
val lower_texts = texts map {_.toString} map { _.trim } filter { _.length != 0 } map { _.toLowerCase }

System.exit(0)
