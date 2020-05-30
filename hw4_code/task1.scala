import java.io._

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import scala.collection.mutable.{Map, Queue, Set}
import org.graphframes._
import scala.util.matching.Regex


object task1 {
	def build_pair(value_list: Iterable[String]): List[Tuple2[String, String]] = {
		var ans : List[Tuple2[String, String]] = List()
		val l = value_list.toList.distinct.sorted
		for(s1 <- 0 to l.size-1){
			for(s2 <- s1+1 to l.size-1){
				val one = Tuple2(l.apply(s1), l.apply(s2))
				val one_reverse = Tuple2(l.apply(s2), l.apply(s1))
				ans +:= one
				ans +:= one_reverse
			}
		}
		ans
	}

	def main(args: Array[String]): Unit = {
		val t1 = System.currentTimeMillis()
		val threshold = args.apply(0).toInt
		val input_file = args.apply(1)
		val output_file = args.apply(2)
		val conf = new SparkConf().setAppName("inf553").setMaster("local[3]")
		val sc = new SparkContext(conf)
		sc.setLogLevel("WARN")
		val sql_sc = new SQLContext(sc)
		val re = new Regex(",")
		val data = sc.textFile(input_file).map(x=>re.split(x)).filter(x=>x.apply(0) != "user_id")
		val user_piar = data.map(x=>Tuple2(x.apply(1), x.apply((0)))).groupByKey().mapValues(x=>build_pair(x)).flatMap(
			x=> x._2).map(x=>Tuple2(x,1)).reduceByKey(_+_).filter(x=> x._2 >= threshold).map(x=>x._1)
		val users = user_piar.flatMap(x=> List(x._1,x._2)).distinct().map(x=>Tuple1(x))
		val vertices = sql_sc.createDataFrame(users).toDF("id")
		val edges = sql_sc.createDataFrame(user_piar).toDF("src", "dst")
		val graph = GraphFrame(vertices, edges)
		val result = graph.labelPropagation.maxIter(5).run().select("id", "label").rdd.map(
			x=>Tuple2(x.apply(1).toString, x.apply(0).toString)).groupByKey().map(x=>x._2.toList.sorted).sortBy(
			x=>x.apply(0)).sortBy(x=>x.size).collect()
		val w = new PrintWriter(new File(output_file))
		for (item <- result){
			for(i<- 0 to item.size-1){
				w.write("'"+item.apply(i)+"'")
				if(i != item.size-1) {
					w.write(", ")
				}
			}
			w.write("\n")
		}
		w.flush()
		w .close()
		sc.stop()
		println("Duration:"+(System.currentTimeMillis()-t1)/1000)

	}
}
