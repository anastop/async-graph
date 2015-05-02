package async_graph

import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill, Props}
import scala.concurrent.duration._
import com.typesafe.config._


class Coordinator(nworkers: Int, gi: GraphInfo) extends Actor with ActorLogging {
    import Worker._
    import Messages._
    import Graph._
    import context.dispatcher

    var workers = Map.empty[WorkerId, ActorRef] 
    var partialEdgeCounts = Map.empty[WorkerId, EdgeKey]
    var lastUpdates = Map.empty[WorkerId, Long]
    var ponged = Set.empty[WorkerId]
    var origSender:Option[ActorRef] = None
    var chunksProcessed = 0
    var edgesRead, totalUpdates, hblatency, tic, start, stop = 0L
    val conf = ConfigFactory.load() 
    val hbfirst = conf.getLong("params.coordinator.heartbeats.first") 
    val hbperiod = conf.getLong("params.coordinator.heartbeats.period")
    val hbminround = conf.getLong("params.coordinator.heartbeats.minroundtrip")
    var activity_checks = conf.getInt("params.coordinator.activity_checks")

    override def preStart() {
    // create workers 
    workers = (0 to (nworkers-1)) map { i =>
        val worker = context.actorOf(workerProps(i,nworkers,gi), "worker"+i)
        context.watch(worker)
        (i -> worker)
    } toMap
    
    log.info(s"Created ${workers.keys.size} workers")

  }

  def receive = initializing
    
  /**
   * INITIALIZING behaviour
   */
    def initializing: Receive = {

    case StartWorld =>
        origSender = Some(sender)
        workers.values foreach { _ ! CurrentWorkers(workers) }
        log.info(s"Broadcasted workers")
        // cyclically distribute chunks to workers  
        (gi.firstChunk to gi.lastChunk) foreach { c =>
            workers(c % nworkers) ! ReadChunk(c)
        }

        log.info(s"Distributed chunks")
    
        // ACKs for each chunk
        case ProcessedChunk =>
            chunksProcessed += 1
            if ( chunksProcessed == (gi.lastChunk-gi.firstChunk+1))
                workers.values foreach { _ ! CountEdges }

        // process EdgeCount messages only if the total number of
        // accumulated edges are < gi.edges
        case EdgeCount(wid, nedges) if edgesRead < gi.nedges =>
            partialEdgeCounts += (wid -> nedges)
            edgesRead = partialEdgeCounts.values.sum

        if ( edgesRead == gi.nedges ) {
            log.info(s"$edgesRead edges read by workers (total in graph: ${gi.nedges}")
            log.info(s"Signaling workers to start computation")
            workers.values foreach { _ ! StartWorking }
            start = System.currentTimeMillis
            context.system.scheduler.scheduleOnce(hbfirst.milliseconds) {
                workers.values foreach { _ ! Ping }    
                tic = System.currentTimeMillis 
                log.info(s"-> HEARTBEATING")
            }    
            context.become(heartBeating)
        } else if ( edgesRead < gi.nedges ) {
            // send request to all workers again 
            workers.values foreach { _ ! CountEdges }    
        }      
    }

    /**
     * HEARTBEATING behaviour
     */
    def heartBeating:Receive = {

        case Pong(wid, upds) => 
            ponged += wid 
            lastUpdates += (wid->upds)

        if ( ponged.size == workers.keys.size ) {
            ponged = ponged.empty
            totalUpdates = lastUpdates.values.sum
            lastUpdates = lastUpdates.empty
        
            hblatency = System.currentTimeMillis - tic 
            if ( hblatency < hbminround ) {
                log.info(s"Roundtrip seems minimal ($hblatency < $hbminround). Going to check activity.")
                workers.values foreach { _ ! ReportActivity }
                context.become(checkingActivity)
            } else {
                val hbnext = math.min(hbperiod, hblatency)
                context.system.scheduler.scheduleOnce(hbnext.milliseconds) {
                    workers.values foreach { _ ! Ping }
                    tic = System.currentTimeMillis
                    log.info(s"Heartbeating... (last roundtrip: ${hblatency/1000.0} sec, updates since last time: $totalUpdates)")
                }
            }
        }
    }

    /**
    * CHECKINGACTIVITY behaviour
    */
    def checkingActivity:Receive = {

        case LastUpdates(wid, upds) =>
            lastUpdates += (wid->upds)

        // all workers sent
        if ( lastUpdates.keys.size == workers.keys.size ) {

    `       // continue until totalUpdates reaches 0, and then 
            // retry for "activity_checks" times to assure that it 
            // stays at that value
            totalUpdates = lastUpdates.values.sum
            lastUpdates = lastUpdates.empty

            log.info(s"Updates since last time: $totalUpdates")
            if ( totalUpdates == 0 ) {
                log.info(s"Zero updates since last time. Checking for $activity_checks times more")
              
                if ( activity_checks > 0 ) {
                    activity_checks -= 1
                    context.system.scheduler.scheduleOnce(100.milliseconds) {
                        workers.values foreach { _ ! ReportActivity }
                    }
                } else { 
                    stop = System.currentTimeMillis
                    log.info(s"Execution time (milliseconds): ${stop-start}")
                    workers.values foreach { _ ! PoisonPill }
                    origSender.get ! Result((stop-start).toString)
                    context.stop(self)
                }
            } else {
                context.system.scheduler.scheduleOnce(10.milliseconds) {
                    workers.values foreach { _ ! ReportActivity }
                }
            }
        }
    }

    def workerProps(id: WorkerId, nworkers: Int, gi: GraphInfo): Props = 
        Props(new Worker(id, nworkers, gi))
}