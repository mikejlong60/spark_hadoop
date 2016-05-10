import org.apache.spark.util.StatCounter

class NAStatCounter extends Serializable {
  val stats = new StatCounter()
  var missing = 0

  def add(x: Double): NAStatCounter = {
    if (java.lang.Double.isNaN(x))
      missing += 1
    else
      stats.merge(x)
    this
  }

  def merge(other: NAStatCounter): NAStatCounter = {
    stats.merge(other.stats)
    missing += other.missing
    this
  }

  override def toString = s"stats: $stats NaN: $missing"
}

object NAStatCounter extends Serializable {
  def apply(x: Double) = new NAStatCounter().add(x)
}


import org.apache.spark.rdd.RDD

def statsWithMissing(rdd: RDD[Array[Double]]): Array[NAStatCounter] = {
  val nastats = rdd.mapPartitions((iter: Iterator[Array[Double]]) => {
    if (iter.nonEmpty) {
      val nas: Array[NAStatCounter] = iter.next().map(d => NAStatCounter(d))
      iter.foreach(arr => {
        nas.zip(arr).foreach { case (n, d) => n.add(d) }
      })
      Iterator(nas)
    } else {
      Iterator.empty
    }
  })

  nastats.reduce((n1, n2) => {
    n1.zip(n2).map { case (a, b) => a.merge(b)}
  })
}

def naz(d: Double) = if (Double.NaN.equals(d)) 0.0 else d

case class Scored(md: MatchData, score: Double)

val ct = parsed.map(md => {
  val score = Array(2,5,6,7,8).map(i => naz(md.scores(i))).sum
  Scored(md, score)
})
