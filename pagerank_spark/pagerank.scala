
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.commons.lang.StringUtils;

/**
  * Computes the PageRank of WIKI pages from an input file.
  */
object scala1 {

  def main(args: Array[String]) {

    if (args.length < 1) {// Check for arguments
      System.err.println("SparkPageRank <file> missing")
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("PageRank").setMaster("local")
    val itrs =10;//Set the number of iterations

    val ctx = new SparkContext(sparkConf)
    val lines = ctx.textFile(args(0), 1)
    val l = lines.map(l => l + "vvv<title>null</title> v<text vvxx[[null]]xcv </text>")
    val N = l.flatMap(l => l.split("\\n ")).map(word => (word, 1)).reduceByKey(_ + _).count()//Count total number of pages from each line

    //println(l.first())
   //seperate to key value pair
    val links = lines.map{ s =>
         // if(s==null) s.replace(null, ' ')
          val part1 = (s.substring(s.lastIndexOf("<title>") + 7, s.lastIndexOf("</title>"))) //Capture title
          val t = StringUtils.substringBetween(s, "<text", "</text>")
          val part2 = (StringUtils.substringsBetween(t, "[[", "]]"))//.filter(_!=null) //capture links

      (part1,part2)//seperate to parts

    }.cache()//cache the output




    var ranks = links.mapValues(v => 1.0/N)//Inilialize rank with 1/(Number of documents)
    var map1= links.mapValues(v => 1.0/N)//Initialize

    for (i <- 1 to itrs) {//loop with 10 iterartions to calculate the pagerank
      val contr = links.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contr.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)//damping factor is .85
      map1=ranks.map(s=>s.swap).sortByKey(false,1).map(s=>s.swap)// Sorting the value in descending

    }

    val output = map1.collect()
    output.foreach(tup => println(tup._1 + "  " + tup._2 + "."))

    ctx.stop()

  }
}