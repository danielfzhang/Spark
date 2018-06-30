import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SetSimJoin {
  def main(args: Array[String]) {

    //input parameters
    val inputFile = args(0)
    val outputFolder = args(1)
    val threshold = args(2).toDouble

    //configuration
    val conf = new SparkConf().setAppName("SetSimJoin")
    val sc = new SparkContext(conf)

    def prefixLen(x:Int):Int = x - (x * threshold).ceil.toInt + 1                 //prefix length

    def overlap(x:Array[Int], y:Array[Int]):Double =                              //min number of common elements required
      (math rint (threshold/(1+threshold)) * (x.length+y.length) * 100) /100

    def sim(x:Array[Int], y:Array[Int]):Double = {                                //jaccard similarity
      val intersectLen = x.intersect(y).length
      intersectLen.toDouble / (x.length + y.length - intersectLen)
    }

    //main computation
    val record = sc.textFile(inputFile).mapPartitions(                             //turn input data to (rid, [attributes]) pairs, and cache them
                  _.map( _.split(" ").map(_.toInt) )
                ).cache()

    val G = sc.broadcast(                                                          //sort tokens in ascending order by frequency, then broadcast it
            record.flatMap( _.drop(1).map((_,1)) ).
            reduceByKey(_+_).sortBy(_._2).
            keys.collect.zipWithIndex.toMap
          )

    val result = record.flatMap{x=>
            val sorted = x.drop(1).map(G.value(_)).sorted;                         //convert each attribute to its corresponding index in G, and sort it
            for(i<- 0 until prefixLen(sorted.length) )
              yield ( sorted(i), Array((sorted.length-i-1, x(0), sorted)) )        //replicate a pair of record for each prefix

          }.reduceByKey(_++_).
          filter(_._2.length > 1).                                                 //drop those sample no more than 2
          flatMap{ x=>
            val arr = x._2;
            val similarity = for{
              i<- 0 until arr.length
              j<- i+1 until arr.length
              min_len = arr(i)._3.length.min(arr(j)._3.length)
              if(min_len >= min_len*threshold)                                     //length filter
              if(1 + arr(i)._1.min(arr(j)._1)  >= overlap(arr(i)._3, arr(j)._3))   //positional filter
            } yield ((arr(i)._2, arr(j)._2), sim(arr(i)._3, arr(j)._3));           //compute jaccard similarity for each pairs

            similarity.filter(_._2>=threshold)                                     //keep those pairs of rid with sim >= threshold

          }.distinct.                                                              //drop duplicates
          sortByKey().
          mapPartitions( _.map(x => x._1 + "\t" + x._2) )                          //format the result

    result.saveAsTextFile(outputFolder)
  }
}
