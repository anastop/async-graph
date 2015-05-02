package async_graph

import org.scalatest._

class GraphUtilSpec extends FlatSpec {
  import Graph._

  "A graph chunk" should "be read properly" in {
    val el = readEdgesFromFile("/home/anastop/Graphs/bsp/USA-road-d.BAY_800/x000")
    val gp = new GraphPart
    el foreach { gp.addEdge(_) }
    gp.printAl
  }

  it should "return the right neighbors" in {
  	val gp = new GraphPart 
  	gp.addEdge(Edge(1,2,0.0))
  	gp.addEdge(Edge(1,5,0.0))
  	gp.addEdge(Edge(2,1,0.0))
  	gp.addEdge(Edge(2,7,0.0))
  	gp.addEdge(Edge(4,5,0.0))
  	gp.addEdge(Edge(4,4,0.0))


  	println("Adjacency lists:")
  	gp.printAl

  	for (v <- gp.al.keys) {
	  print(s"Neighbors of $v:")
	  gp.neighbors(v) foreach { n => print(s" $n") }
	  println()
  	}	
  }
}
