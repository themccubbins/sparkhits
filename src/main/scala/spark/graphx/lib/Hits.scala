package spark.graphx.lib

import scala.reflect.ClassTag

import org.apache.spark.Logging
import org.apache.spark.graphx.Graph

/**
  * HITS (Hyperlink-Induced Topic Search) Algorithm implementation. The HITS algorithm analyzes
  * a graph based on two values for each node, a 'hub' value and a 'authority' value. The hub value
  * indicates a page that links to many other pages, such as a directory service in the Web. A node
  * with high authority is linked to by high-valued hubs.
  *
  * @see https://en.wikipedia.org/wiki/HITS_algorithm
  *
  */

object Hits extends Logging{
  /**
    * Run Hits for a fixed number of iterations returning a Graph with vertex attributes
    * containing the (authority, hub) values and leaving the edge attributes unchanged.
    *
    * @tparam VD the original vertex attribute (not used)
    * @tparam ED the original edge attribute (returned unchanged)
    * @param graph the graph on which to compute Hits
    * @param numIter the number of iterations of Hits to run
    * @param normalize whether or not to normalize authority and hub on each iteration such that
    *                  the resulting authority vector and hub vector have magnitude 1. Normalization
    *                  requires two extra passes over the data per iteration and therefore may be significantly
    *                  slower.
    * @return the graph containing vertex attributes containing the (authority, hub) values
    *         and leaving the edge attributes unchanged.
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

      // Compute the auth updates. Each node's new auth score is
      // equal to the sum of the hub Scores of each node that points to it.
      // We want to keep the original hub values unchanged
      // So we send the original hub values to every node as well.
      // We want to add all the hub values in to a new auth value
      // at each dest, but we can just pick one hub value because they should all be the same
      val authUpdates = rankGraph.aggregateMessages[Authhub](
        sendMsg = {triplet => {
          triplet.sendToDst(Authhub(triplet.srcAttr.hub, triplet.dstAttr.hub))
          triplet.sendToSrc(Authhub(0, triplet.srcAttr.hub))
        }},
        mergeMsg = {(a, b) => Authhub(a.auth+b.auth, a.hub) }
      )

      //If we wish to normalize, then divide each auth by the norm of all auths
      val normedAuthUpdates = if(normalize) {
        val norm = Math.sqrt(authUpdates.map(v=>v._2.auth*v._2.auth).reduce(_+_))
        authUpdates.map(v=>(v._1,Authhub(v._2.auth/norm,v._2.hub)))
      } else authUpdates

      //form the intermediate graph with the new auth values. It might be that
      //a node is isolated and had no meesages, in that case set the new auth to 0

      val authUpdatedGraph = Graph(normedAuthUpdates, rankGraph.edges, Authhub(0,0))

      //Hub updates are similar to auth updates, except the new hub score
      //is equal to the sum of the auth scores of each node that it points to.
      //Here, we keep the auth scores the same.
      val hubUpdates = authUpdatedGraph.aggregateMessages[Authhub](
        sendMsg = {triplet => {
          triplet.sendToSrc(Authhub(triplet.srcAttr.auth, triplet.dstAttr.auth))
          triplet.sendToDst(Authhub(triplet.dstAttr.auth, 0))
        }},
        mergeMsg = {(a, b) => Authhub(a.auth, a.hub+b.hub) }
      )

      //normalize, if desired
      val normedHubUpdates = if(normalize) {
        val norm = Math.sqrt(hubUpdates.map(v=>v._2.hub*v._2.hub).reduce(_+_))
        hubUpdates.map(v=>(v._1,Authhub(v._2.auth,v._2.hub/norm)))
      } else hubUpdates

      prevRankGraph = rankGraph

      //form the new graph for this iteration.
      rankGraph = Graph(normedHubUpdates,prevRankGraph.edges, Authhub(0,0))

      logInfo(s"Hits finished iteration $iteration.")

      //drop all the intermediate graphs. Only the edges though, since we just pass
      //the edges through
      authUpdatedGraph.vertices.unpersist(false)
      prevRankGraph.vertices.unpersist(false)

      iteration += 1
    }

    //Flatten the graph back to just (double,double) pairs vs. our intermediate case class
    val flattenedRankGraph = rankGraph.mapVertices((vid,authhub)=>(authhub.auth, authhub.hub))

    flattenedRankGraph
  }

  //An intermediate case class for storage of auth and hub values for one node.
  case class Authhub(val auth:Double, val hub:Double)
}
