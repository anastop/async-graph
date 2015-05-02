package async_graph

import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill, Props, Stash}

object Worker {
    type WorkerId = Int
}

class Worker(me: Worker.WorkerId, nworkers: Int, gi: GraphInfo) extends Actor with Stash with ActorLogging {
    import Worker._
    import Messages._
    import Graph._

    var cc = Map.empty[VertexKey, VertexValue]     // CC map for local vertices
    var allworkers = Map.empty[WorkerId, ActorRef]
    var gp = new GraphPart  
    var lastUpdates = 0L
    val f = "%0" + gi.lastChunk.toString.length + "d"

    def receive = initializing

    /**
     * INITIALIZING behaviour
     */
    def initializing: Receive = {
        // construct workers list (excluding myself)
        case CurrentWorkers(w) =>
        //workers = w filterKeys { _ != me }
            allworkers = w
        context.become(populatingEdges)
    }

    /**
     * POPULATINGEDGES behaviour
     */
    def populatingEdges: Receive = {
    
        // get next chunk to read
        case ReadChunk(i) =>
        val file = gi.path + gi.prefix + f.format(i) 

        // read the edge list of the chunk and split into two partitions: 
        // the one containing local edges, and the one that needs to be sent
        // to other workers
        val edge_partitions = readEdgesFromFile(file) partition ( workerForEdge(_) == self )

        // populate local graph partition from local edges
        edge_partitions._1 foreach { e => gp.addEdge(e) }
        // send non-local edges to owning workers
        edge_partitions._2 foreach { e => workerForEdge(e) ! AddEdge(e) }
        context.parent ! ProcessedChunk

        case AddEdge(e) => 
            gp.addEdge(e)

        case CountEdges => 
            sender ! EdgeCount(me, gp.nedges)

        case StartWorking =>
            // initialize cc's to local vertices
            for (v <- gp.al.keys; n <- gp.neighbors(v)) {
                cc = cc + (v -> v.toDouble)
                workerForVertex(n) ! CC(n, cc(v))
            }
            unstashAll()
            context.become(working)
      
        // stash, just in case other workers have already switched to the "working"
        // state, sending CC messages to us, while we are still processing the "StartWorking"
        // message
        case CC(_,_) => stash()
        
        case Ping => stash()
    }

    /**
     * WORKING behaviour
     */
    def working: Receive = {
        case CC(v,newval) => 
            if ( cc(v) < newval ) {
                doUpdateCC(v, newval)
                lastUpdates += 1 
            } 
     
        case Ping =>
            sender ! Pong(me, lastUpdates)
            lastUpdates = 0

        case ReportActivity =>
            sender ! LastUpdates(me, lastUpdates)
            lastUpdates = 0
    }

    def doUpdateCC(v: VertexKey, newval: VertexValue) {
        cc += (v->newval)
        gp.neighbors(v) foreach { n => workerForVertex(n) ! CC(n, newval) }
    }
    def workerForVertex(v: VertexKey) = allworkers((v%nworkers).toInt)
    def workerForEdge(e: Edge) = workerForVertex(e.source)  
}