package spark.graphx.lib

import org.scalatest.{FunSuite, Matchers}

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}


class HitsSuite extends FunSuite with Matchers{


  val Delta = 0.0000001
  //very simple test taken from http://www.math.cornell.edu/~mec/Winter2009/RalucaRemus/Lecture4/lecture4.html
  test("run HITS 3 node test") {
    val sc = new SparkContext("local[1]","hitstest")
    val vertices = sc.parallelize(List((1l,1),(2l,1),(3l,1)))
    val edges = sc.parallelize(List(Edge(1l,3l,1),Edge(2l,3l,1)))
    val g = Graph(vertices, edges)
    val result = Hits.run(g,1,true).vertices.collect().toMap
    result.get(1l).get._1 should equal (0.0 +- Delta)
    result.get(1l).get._2 should equal (1.0/Math.sqrt(2) +- Delta)
    result.get(2l).get._1 should equal (0.0 +- Delta)
    result.get(2l).get._2 should equal (1.0/Math.sqrt(2) +- Delta)
    result.get(3l).get._1 should equal (1.0 +- Delta)
    result.get(3l).get._2 should equal (0.0 +- Delta)

    val result2 = Hits.run(g,1).vertices.collect().toMap
    result2.get(1l).get._1 should equal (0.0 +- Delta)
    result2.get(1l).get._2 should equal (2.0 +- Delta)
    result2.get(2l).get._1 should equal (0.0 +- Delta)
    result2.get(2l).get._2 should equal (2.0 +- Delta)
    result2.get(3l).get._1 should equal (2.0 +- Delta)
    result2.get(3l).get._2 should equal (0.0 +- Delta)

    sc.stop()
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
    val resultOneIter = Hits.run(g,1,true).vertices.map(v => (vNames.get(v._1).get, v._2))
    val resultOneIterAuth = resultOneIter.map(tup => (tup._1, tup._2._1))
    val resultOneIterHub = resultOneIter.map(tup => (tup._1, tup._2._2))

    val resultTwoIter = Hits.run(g,2,true).vertices.map(v => (vNames.get(v._1).get, v._2))
    val resultTwoIterAuth = resultTwoIter.map(tup => (tup._1, tup._2._1))
    val resultTwoIterHub = resultTwoIter.map(tup => (tup._1, tup._2._2))

    val resultThreeIter = Hits.run(g,3,true).vertices.map(v => (vNames.get(v._1).get, v._2))
    val resultThreeIterAuth = resultThreeIter.map(tup => (tup._1, tup._2._1))
    val resultThreeIterHub = resultThreeIter.map(tup => (tup._1, tup._2._2))

    val resultAuths = resultOneIterAuth.join(resultTwoIterAuth).join(resultThreeIterAuth)
      .map(tup => (tup._1, List(tup._2._1._1, tup._2._1._2, tup._2._2))).collect().toMap

    val resultHubs = resultOneIterHub.join(resultTwoIterHub).join(resultThreeIterHub)
      .map(tup => (tup._1, List(tup._2._1._1, tup._2._1._2, tup._2._2))).collect().toMap

    val trueAuths = Map(
      "A" -> List(0.13736056,0.13732008,0.13492265),
      "B" -> List(0.13736056,0.17165010,0.17919414),
      "C" -> List(0.27472113,0.18881511,0.16443698),
      "D" -> List(0.41208169,0.39479524,0.40687611),
      "E" -> List(0.41208169,0.39479525,0.38579445),
      "F" -> List(0.41208169,0.49778531,0.51228443),
      "G" -> List(0.27472113,0.18881512,0.17708597),
      "H" -> List(0.54944226,0.56644536,0.56077225))

    val trueHubs = Map(
      "A" -> List(0.48853197,0.51148238,0.51984395),
      "B" -> List(0.29311918,0.26476735,0.25401466),
      "C" -> List(0.39082557,0.38511614,0.37880674),
      "D" -> List(0.24426598,0.20459295,0.19272624),
      "E" -> List(0.43967877,0.43927310,0.43787992),
      "F" -> List(0.14655959,0.13840111,0.14251404),
      "G" -> List(0.48853197,0.51148238,0.51836712),
      "H" -> List(0.09770639,0.06619184,0.06202683))


    trueAuths.keys.foreach(node =>{
      trueAuths.get(node).get.zip(resultAuths.get(node).get)
        .foreach(tup=>{
          println(tup)
          tup._1 should equal (tup._2 +- Delta)
        })
    })

    trueHubs.keys.foreach(node =>{
      trueHubs.get(node).get.zip(resultHubs.get(node).get)
        .foreach(tup=>{
          println(tup)
          tup._1 should equal (tup._2 +- Delta)
        })
    })

    sc.stop()
  }
}