val in = "data/enron-spam-ham/*"
val out = "output/crawl"
val separator = java.io.File.separator

val file_contents = sc.wholeTextFiles(in)

//file_contents.take(1).foreach(println)
val crawled = file_contents.map{
  case (path, text) =>
    val lastSep = path.lastIndexOf(separator)
    val path2 =
      if (lastSep < 0) path.trim
      else path.substring(lastSep+1, path.length).trim
    val text2 = text.trim.replaceAll("""\s*\n\s*""", " ")
    (path2, text2)
}

crawled.saveAsTextFile(out)

println("Now that we have our "crawl" data, let's compute the inverted index for it.")

val iiout = "output/inverted-index"
val lineRE = """^\s*\(([^,]+),(.*)\)\s*$""".r

println("Parse the input lines using the regex. If that fails, return an empty tuple ("", ""), which can be filtered out later.")

val ii_input = sc.textFile(out).map{
  case lineRE(name, text) => (name.trim, text.tolowerCase)
  case badLine => Console.err.println(s"unexpected line: $badLine")
  ("", "")
}

val ii = ii_input.flatmap{
  case (path, text) => 
    text.trim.split("""[^\p{IsAlphabetic}]+""").map(word => (word, path))
}.map{
  case (word, path) => ((word, path), 1)
}.reduceByKey{
  (count1, count2) => count1 + count2
}.map{
  case ((word, path), n) => (word, (path, n))
}.groupByKey

ii.mapValues(iterator => iterator.mkString(", ")).saveAsTextFile(iiout)


System.exit(0)
