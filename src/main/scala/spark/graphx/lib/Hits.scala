package spark.graphx.lib

import org.apache.spark.Logging
import org.apache.spark.graphx.{TripletFields, VertexId, Graph}

import scala.reflect.ClassTag

object Hits extends Logging{
  /**
    * Run Hits for a fixed number of iterations returning ...
    *
    * @tparam VD the original vertex attribute (not used)
    * @tparam ED the original edge attribute (returned unchanged)
    * @param graph the graph on which to compute PageRank
    * @param numIter the number of iterations of PageRank to run
    * @param normalize whether or not to normalize authority and hub on each iteration
    * @return the graph containing ...
    *
    */
  def run[VD: ClassTag, ED: ClassTag](
    graph: Graph[VD, ED], numIter: Int, normalize: Boolean = false): Graph[(Double, Double), ED] =
  {

    // Initialize the Hits graph with each vertex attribute having
    // authority = 1 and hub = 1
    var rankGraph: Graph[Authhub,ED] = graph.mapVertices((vid,vd) => Authhub(1.0,1.0))

    var iteration = 0
    var prevRankGraph: Graph[Authhub,ED] = null
    while (iteration < numIter) {
      rankGraph.cache()

      // Compute the ...
      // ...
      val authUpdates = rankGraph.aggregateMessages[Authhub](
        sendMsg = {triplet => {
          triplet.sendToDst(Authhub(triplet.srcAttr.hub, triplet.dstAttr.hub))
          triplet.sendToSrc(Authhub(0, triplet.srcAttr.hub))
        }},
        mergeMsg = {(a, b) => Authhub(a.auth+b.auth, a.hub.max(b.hub)) }
      )

      val normedAuthUpdates = if(normalize) {
        val norm = Math.sqrt(authUpdates.map(v=>v._2.auth*v._2.auth).reduce(_+_))
        authUpdates.map(v=>(v._1,Authhub(v._2.auth/norm,v._2.hub)))
      } else authUpdates

      val authUpdatedGraph = Graph(normedAuthUpdates, rankGraph.edges, Authhub(0,0))

      authUpdatedGraph.vertices.foreach(println)

      val hubUpdates = authUpdatedGraph.aggregateMessages[Authhub](
        sendMsg = {triplet => {
          triplet.sendToSrc(Authhub(triplet.srcAttr.auth, triplet.dstAttr.auth))
          triplet.sendToDst(Authhub(triplet.dstAttr.auth, 0))
        }},
        mergeMsg = {(a, b) => Authhub(a.auth.max(b.auth), a.hub+b.hub) }
      )

      val normedHubUpdates = if(normalize) {
        val norm = Math.sqrt(hubUpdates.map(v=>v._2.hub*v._2.hub).reduce(_+_))
        hubUpdates.map(v=>(v._1,Authhub(v._2.auth,v._2.hub/norm)))
      } else hubUpdates

      prevRankGraph = rankGraph

      rankGraph = Graph(normedHubUpdates,prevRankGraph.edges, Authhub(0,0))

      logInfo(s"Hits finished iteration $iteration.")
      prevRankGraph.vertices.unpersist(false)
      prevRankGraph.edges.unpersist(false)

      iteration += 1
    }

    val flattenedRankGraph = rankGraph.mapVertices((vid,authhub)=>(authhub.auth, authhub.hub))

    flattenedRankGraph
  }

  case class Authhub(val auth:Double, val hub:Double)
}
