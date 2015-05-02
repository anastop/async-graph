package async_graph

object Graph {
    type VertexKey = Long
    type EdgeKey = Long
    type EdgeValue = Double
    type VertexValue = Double 

    case class Edge(source: VertexKey, target: VertexKey, var weight: EdgeValue)
    case class OutEdge(target: VertexKey, var weight: EdgeValue)
    case class Vertex(id: VertexKey, var weight: VertexValue)

    implicit def string2VertexKey(s: String): VertexKey = 
        augmentString(s).toInt
    implicit def string2EdgeValue(s: String): EdgeValue = 
        augmentString(s).toDouble

    /**
    * Reads a file line-by-line, parses edges and 
    * returns a list containing them. 
    */
    def readEdgesFromFile(file: String): List[Edge] = {
        var edgelist = List.empty[Option[Edge]]
        val liter = scala.io.Source.fromFile(file, "UTF-8").getLines
        for ( l <- liter ) {
            val edge = l.split("\\s+") match {
                case Array(x,y,z) if !x.startsWith("#") => Some(Edge(x,y,z))
                case Array(x,y) if !x.startsWith("#") => Some(Edge(x,y,0.0))
                case _ => None
            }
            edgelist ::= edge 
        } 
        edgelist.flatten    // to exclude None's
    }
} //!object Graph

case class GraphInfo(
    nvertices: Graph.VertexKey,
    nedges: Graph.EdgeKey,
    path: String, 
    prefix: String,
    firstChunk: Int, 
    lastChunk: Int) 

/**
 * Represents a partition of a graph, i.e. a set of vertices along with
 * all their outgoing edges
 */
class GraphPart {
    import Graph._

    // adjacency list for local graph part
    var al = Map.empty[VertexKey, Set[OutEdge]]
    var nedges = 0
    var nvertices = 0
  
    def addEdge(e: Edge) = {
        if (e.source != e.target) {   //ignore loops
            al = al + (e.source -> 
                (al.get(e.source).getOrElse(Set.empty[OutEdge]) + OutEdge(e.target, e.weight))
            )    
            nedges += 1
        }
    }

    def neighbors(v: VertexKey): Set[VertexKey] = 
        al(v) map { oe => oe.target } 

    def printAl = {
        for (v <- al.keys) 
            println(alToString(v))
    }

    def alToString(v: VertexKey): String = {
        var s = "[" + v + "]"
        for (oe <- al(v)) {
            s += "->" + oe.target + "(" + oe.weight + ") "
        }
        s
    }

    def writeGraphPart(file:String) {
        val out = new java.io.PrintWriter(file)
        for (v <- al.keys) {
            out.print("[" + v + "]")
            for (oe <- al(v)) {
                out.print(" ->" + oe.target + "(" + oe.weight + "), ")
            }
            out.println("")
        }
        out.close()
    }
}