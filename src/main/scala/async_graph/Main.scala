package async_graph

import akka.actor.ActorSystem
import akka.actor.Props
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await

/**
 * Arguments:
 * @1: number of graph vertices
 * @2: number of graph edges
 * @3: graph path 
 * @4: graph chunks prefix
 * @5: number of first chunk
 * @6: number of last chunk
 * @7: number of workers
 * @8: number of dispatchers
 * @9: dispatcher throughput
 */
object Main extends App {
    import Messages._

    // timeout that we are willing to wait for the '?' operation
    // to be fulfilled
    val to = 10000.seconds
    implicit val timeout = Timeout(to)

    val Array(nvertices, 
              nedges, 
              path, 
              prefix, 
              firstChunk, 
              lastChunk, 
              workers, 
              dispatchers, 
              throughput) = args 

    val confstr =  """
        akka.actor.default-dispatcher.throughput = """ + throughput + """
        akka.actor.default-dispatcher.fork-join-executor.parallelism-min = """ + dispatchers + """
        akka.actor.default-dispatcher.fork-join-executor.parallelism-max = """ + dispatchers
  

    val customConf = ConfigFactory.parseString(confstr) 
    println(confstr)

    val system = ActorSystem("GraphSystem", ConfigFactory.load(customConf))

    val gi = GraphInfo(nvertices.toLong, 
                       nedges.toLong, 
                       path, 
                       prefix,
                       firstChunk.toInt, 
                       lastChunk.toInt)

    val coord = system.actorOf(Props(new Coordinator(workers.toInt, gi)), "coord")
    val res = Await.result( (coord ? StartWorld).mapTo[Result], to )
    val Result(ms) = res
    println(s"Execution time: $ms millis")
    system.shutdown() 
}