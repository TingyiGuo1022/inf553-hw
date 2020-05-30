import java.io._

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{Map, Queue, Set}
import scala.util.matching.Regex

class Node(v: String, lvl: Int){
	var value: String = v
	var childrens: List[String] = List()
	var short_path_num : Double = 0.0
	var level : Int = lvl

}

object task2 {

	def build_pair(value_list: Iterable[String]): List[Tuple2[String, String]] = {
		var ans : List[Tuple2[String, String]] = List()
		val l = value_list.toList.distinct.sorted
		for(s1 <- 0 to l.size-1){
			for(s2 <- s1+1 to l.size-1){
				val one = Tuple2(l.apply(s1), l.apply(s2))
				ans +:= one
			}
		}
		ans
	}

	def cal_credit(node: String, tree: Map[String, Node]) : Double = {
		if (tree.apply(node).childrens.size == 0)  return 1.0
		var sum_credit = 1.0
		for(child <- tree.apply(node).childrens){
			var child_credit = cal_credit(child, tree)
			child_credit = child_credit*(tree.apply(node).short_path_num/tree.apply(child).short_path_num)
			sum_credit += child_credit
		}
		sum_credit
	}

	def cal_bet(edges: Map[String, List[String]], node:
	String): List[Tuple2[Tuple2[String,String], Double]] = {
		var q = new Queue[String]
		q += node
		var tree: Map[String, Node] = Map()
		tree += (node -> new Node(node, 0))
		tree.apply(node).short_path_num = 1.0

		while (q.nonEmpty){
			val cur = q.dequeue()
			for(n <- edges.apply(cur)){
				if(!tree.contains(n)){
					q += n
					val child = new Node(n, tree.apply(cur).level+1)
					child.short_path_num += tree.apply(cur).short_path_num
					tree.apply(cur).childrens +:= n
					tree += (n -> child)
				}
				else if(tree.contains(n) && tree.apply(cur).level == tree.apply(n).level-1){
					tree.apply(n).short_path_num += tree.apply(cur).short_path_num
					tree.apply(cur).childrens +:= n
				}
			}
		}

		var ans: List[Tuple2[Tuple2[String,String], Double]] = List()
		for(item <- tree.keys){
			for(c <- tree.apply(item).childrens){
				val c_credit = cal_credit(c,tree)*(tree.apply(item).short_path_num/tree.apply(c).short_path_num)
				val pair = List(item, c).sorted
				val btw = Tuple2(Tuple2(pair.apply(0), pair.apply(1)), c_credit)
				ans +:= btw
			}
		}
		ans
	}

	def build_commuity(edges: Map[String, List[String]]): List[Set[String]]={
		var commuities: List[Set[String]] = List()
		var users = edges.keys.toSet
		var visited: Set[String] = Set()
		while (users.size != 0){
			var commuity: Set[String] = Set()
			var q = new Queue[String]
			q += users.last
			while(q.nonEmpty){
				val prev = q.dequeue()
				commuity.add(prev)
				for(n <- edges.apply(prev)){
					if(!visited.contains(n)){
						q += n
						commuity.add(n)
						visited.add(n)
					}
				}
			}
			commuities ::= commuity
			users = users.diff(commuity)
		}
		commuities
	}

	def cal_modularity(cur_edges: Map[String, List[String]], m: Double, edges: Map[String, List[String]]): Double={
		val commuities = build_commuity(cur_edges)
		var G = 0.0
		for (commuity <- commuities){
			var g = 0.0
			for(i<-commuity){
				for(j <- commuity){
					if(i != j){
						val ki = edges.apply(i).size.toFloat
						val kj = edges.apply(j).size.toFloat
						var Aij = 0.0
						if (edges.apply(i).contains(j)) {Aij = 1.0}
						g += (Aij - (ki*kj)/(2*m))
					}
				}
			}
			G += g
		}
		G/(2*m)
	}



	def main(args: Array[String]): Unit = {
		val t1 = System.currentTimeMillis()
		val threshold = args.apply(0).toInt
		val input_file = args.apply(1)
		val output_file = args.apply(2)
		val output_file_2 = args.apply(3)
		val conf = new SparkConf().setAppName("inf553").setMaster("local[4]")
		val sc = new SparkContext(conf)
		sc.setLogLevel("WARN")
		val re = new Regex(",")
		val data = sc.textFile(input_file).map(x=>re.split(x)).filter(x=>x.apply(0) != "user_id")
		val user_piar = data.map(x=>Tuple2(x.apply(1), x.apply((0)))).groupByKey().mapValues(x=>build_pair(x)).flatMap(
			x=> x._2).map(x=>Tuple2(x,1)).reduceByKey(_+_).filter(x=> x._2 >= threshold).map(x=>x._1)
		val users = user_piar.flatMap(x=> List(x._1,x._2)).distinct()
		val edges_rdd = user_piar.collect()
		var edges : Map[String, List[String]] = Map()
		var m = 0
		for(item <- edges_rdd){
			m += 1

			if(!edges.contains(item._1)){
				edges += (item._1 -> List())
			}
			if(!edges.contains(item._2)){
				edges += (item._2 -> List())
			}
			edges.apply(item._1) ::= item._2
			edges.apply(item._2) ::= item._1
		}
		val betweenness = users.flatMap(x=>cal_bet(edges, x)).reduceByKey(_+_).mapValues(x=>
			x/2).sortByKey().sortBy(x=> -x._2).collect()

		val w = new PrintWriter(new File(output_file))
		for(item <- betweenness){
			w.write("("+"'"+item._1._1+"'"+", "+"'"+item._1._2+"'"+")"+", "+item._2.toString+"\n")
		}
		w.flush()
		w.close()

		val cur_edges = edges.clone()
		var max_edges = edges.clone()
		var max_G = -1.0
		var count = 0
		while(count < m){
			val betweenness = users.flatMap(x=>cal_bet(cur_edges, x)).reduceByKey(_+_).mapValues(x=>
				x/2).sortByKey().sortBy(x=> -x._2)
			val drop_edge = betweenness.take(1).apply(0)
			cur_edges.put(drop_edge._1._1, cur_edges.apply(drop_edge._1._1).filter(x=> x !=  drop_edge._1._2))
			cur_edges.put(drop_edge._1._2, cur_edges.apply(drop_edge._1._2).filter(x=> x !=  drop_edge._1._1))
			val cur_G = cal_modularity(cur_edges, m, edges)
			if (cur_G > max_G){
				max_G = cur_G
				max_edges = cur_edges.clone()

			}
			count += 1
		}

		val communities = build_commuity(max_edges).map(x=>x.toList.sorted).sortBy(x =>x.apply(0)).sortBy(x=> x.size)
		val w_2 = new PrintWriter(new File(output_file_2))
		for (item <- communities){
			for(i<- 0 to item.size-1){
				w_2.write("'"+item.apply(i).toString+"'")
				if(i != item.size-1) {
					w_2.write(", ")
				}
			}
			w_2.write("\n")
		}
		w_2.flush()
		w_2.close()
		sc.stop()
		println("Duration: " +(System.currentTimeMillis()-t1)/1000)
	}
}

