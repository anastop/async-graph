package async_graph

import akka.actor.ActorRef

object Messages {
    import Worker._
    import Graph._

    case class ReadChunk(id:Int)
    case class ProcessedChunk
    case object CountEdges
    case class CC(v:VertexKey, cc:VertexValue)
    case class CurrentWorkers(workers: Map[WorkerId, ActorRef])
    case class EdgeCount(wid:WorkerId, nedges:EdgeKey)
    case class Finished(wid:WorkerId)
    case object StartWorking
    case class AddEdge(e:Edge)
    case object ReportActivity 
    case class LastUpdates(wid:WorkerId, lastUpdates:Long)
    case object Ping
    case class Pong(wid:WorkerId, lastUpdates:Long)
    case object StartWorld
    case class Result(s:String)
}
