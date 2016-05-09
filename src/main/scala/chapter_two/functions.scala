val rdd3 = sc.textFile("file:///home/vagrant/linkage/block_10.csv")

val rawblocks = sc.textFile("linkage")

val toDouble = (s: String) => if ("?".equals(s)) Double.NaN else s.toDouble

case class MatchData(id1: Int, id2: Int, scores: Array[Double], matched: Boolean)

def parse(line: String) = {
  val pieces = line.split(",")
  val id1 = pieces(0).toInt
  val id2 = pieces(1).toInt
  val scores = pieces.slice(2, 11). map(toDouble)
  val matched = pieces(11).toBoolean
  MatchData(id1, id2, scores, matched)
}


val isHeader = (line: String) => line.contains("id_1")

val head = rawblocks.take(10)


val mds = head.filter(x => !isHeader(x)).map(x => parse(x))

val grouped = mds.groupBy(md => md.matched)

grouped.mapValues(x => x.size).foreach(println)


val noheader = rawblocks.filter(x => !isHeader(x))


val parsed = noheader.map(line => parse(line))

val matchCounts = parsed.map(md => md.matched).countByValue()

val matchCountsSeq = matchCountsSeq.toSeq

matchCountsSeq.sortBy(row => row._2).reverse.foreach(println)

