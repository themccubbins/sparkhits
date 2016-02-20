package spark.graphx.lib

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph, Edge}
import org.scalatest.FunSuite
import scala.collection.mutable.Stack

class HitsSuite extends FunSuite {


  //very simple example from http://www.math.cornell.edu/~mec/Winter2009/RalucaRemus/Lecture4/lecture4.html
  test("run hits") {
    val sc = new SparkContext("local[1]","hitstest")
    val vertices = sc.parallelize(List((1l,1),(2l,1),(3l,1)))
    val edges = sc.parallelize(List(Edge(1l,3l,1),Edge(2l,3l,1)))
    val g = Graph(vertices, edges)
    val result = Hits.run(g,1,true)
    result.vertices.foreach(println)
  }

  //test case from http://www.ijarcce.com/upload/2014/february/IJARCEE9J_a_pooja_comparative.pdf
  test("run hits 2") {
    val sc = new SparkContext("local[1]","hitstest")
    val vertexNames = List("A","B","C","D","E","F","G","H")
    val vIds = vertexNames.map(str => (str, str.hashCode.toLong)).toMap
    val vNames = vertexNames.map(str => (str.hashCode.toLong, str)).toMap

    val vertices = sc.parallelize(vIds.values.map(vid => (vid,1)).toSeq)
    val edges = sc.parallelize(List(
      Edge(vIds.get("A").get,vIds.get("B").get,1),
      Edge(vIds.get("A").get,vIds.get("D").get,1),
      Edge(vIds.get("A").get,vIds.get("E").get,1),
      Edge(vIds.get("A").get,vIds.get("F").get,1),

      Edge(vIds.get("B").get,vIds.get("C").get,1),
      Edge(vIds.get("B").get,vIds.get("H").get,1),

      Edge(vIds.get("C").get,vIds.get("A").get,1),
      Edge(vIds.get("C").get,vIds.get("E").get,1),
      Edge(vIds.get("C").get,vIds.get("H").get,1),

      Edge(vIds.get("D").get,vIds.get("C").get,1),
      Edge(vIds.get("D").get,vIds.get("E").get,1),

      Edge(vIds.get("E").get,vIds.get("F").get,1),
      Edge(vIds.get("E").get,vIds.get("G").get,1),
      Edge(vIds.get("E").get,vIds.get("H").get,1),

      Edge(vIds.get("F").get,vIds.get("D").get,1),

      Edge(vIds.get("G").get,vIds.get("D").get,1),
      Edge(vIds.get("G").get,vIds.get("F").get,1),
      Edge(vIds.get("G").get,vIds.get("H").get,1),

      Edge(vIds.get("H").get,vIds.get("G").get,1)
    ))
    val g = Graph(vertices, edges)
    val result = Hits.run(g,2,true)
    result.vertices.map(v => (vNames.get(v._1).get, v._2)).sortByKey().foreach(println)
  }
}