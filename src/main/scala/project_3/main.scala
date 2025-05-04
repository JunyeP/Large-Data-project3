package project_3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}

object main{
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)

  def LubyMIS(g_in: Graph[Int, Int]): Graph[Int, Int] = {
    // Initialize graph: all vertices start as "undecided" with value 0
    var g = g_in.mapVertices((_, _) => 0)
    
    // Start time for the entire algorithm
    val totalStartTime = System.currentTimeMillis()
    
    // Add iteration counter
    var iterationCount = 0
    
    // Store iteration statistics for final summary
    var iterationStats = List[(Int, Long, Long)]() // (iteration, undecided vertices, time in ms)
    
    // Print initial state
    val totalVertices = g.vertices.count()
    println("\n======= LUBY MIS ALGORITHM EXECUTION =======")
    println(s"TOTAL VERTICES: $totalVertices")
    println("ITERATION | UNDECIDED VERTICES | TIME (ms)")
    println("----------|-------------------|----------")
    
    // Run iterations until no undecided vertices remain
    var hasUndecidedVertices = true
    while (hasUndecidedVertices) {
      // Start timing this iteration
      val iterStartTime = System.currentTimeMillis()
      
      // Increment counter
      iterationCount += 1
      
      // Step 1: Get count of remaining undecided vertices
      val remainingCount = g.vertices.filter(_._2 == 0).count()
      
      if (remainingCount == 0) {
        hasUndecidedVertices = false
      } else {
        // Step 2: Each undecided vertex chooses a random value
        val randomized = g.mapVertices((vid, attr) => 
          if (attr == 0) (scala.util.Random.nextDouble(), attr) else (0.0, attr)
        )
        
        // Step 3: Select vertices that have a higher random value than all their neighbors
        val selectedForMIS = randomized.aggregateMessages[(Double, Int)](
          triplet => {
            // Only consider edges where both endpoints are undecided
            if (triplet.srcAttr._2 == 0 && triplet.dstAttr._2 == 0) {
              // Send src information to dst if src has higher value
              if (triplet.srcAttr._1 > triplet.dstAttr._1) {
                triplet.sendToDst(triplet.srcAttr)
              }
              // Send dst information to src if dst has higher value
              if (triplet.dstAttr._1 > triplet.srcAttr._1) {
                triplet.sendToSrc(triplet.dstAttr)
              }
            }
          },
          // Keep the max value seen
          (a, b) => if (a._1 > b._1) a else b,
          TripletFields.All
        )
        
        // Create a new graph by mapping vertices
        val verticesWithNeighborInfo = randomized.vertices.leftOuterJoin(selectedForMIS)
          .map { case (vid, ((randVal, attr), optNeighbor)) =>
            // Process based on whether there is a higher-valued neighbor
            val newAttr = if (attr == 0) {
              optNeighbor match {
                case None => 1 // No higher-valued neighbors, add to MIS
                case Some((neighborVal, _)) =>
                  if (randVal > neighborVal) 1 else 0 // Has higher-valued neighbor, compare values
              }
            } else {
              attr // Keep existing value for already decided vertices
            }
            (vid, newAttr)
          }
        
        // Update graph with new MIS vertices
        g = Graph(verticesWithNeighborInfo, g.edges)
        
        // Step 4: Remove neighbors of MIS vertices
        val neighborsOfMIS = g.aggregateMessages[Int](
          triplet => {
            // If source is in MIS, mark destination as not in MIS
            if (triplet.srcAttr == 1 && triplet.dstAttr == 0) {
              triplet.sendToDst(-1)
            }
            // If destination is in MIS, mark source as not in MIS
            if (triplet.dstAttr == 1 && triplet.srcAttr == 0) {
              triplet.sendToSrc(-1)
            }
          },
          // Any message received means vertex is not in MIS
          (a, b) => -1,
          TripletFields.All
        )
        
        // Update vertices using the neighbor information
        val updatedVertices = g.vertices.leftOuterJoin(neighborsOfMIS)
          .map { case (vid, (oldAttr, optNewAttr)) =>
            val finalAttr = optNewAttr match {
              case Some(newAttr) => if (oldAttr == 0) newAttr else oldAttr
              case None => oldAttr
            }
            (vid, finalAttr)
          }
        
        // Create updated graph
        g = Graph(updatedVertices, g.edges)
      }
      
      // End timing this iteration
      val iterEndTime = System.currentTimeMillis()
      val iterDuration = iterEndTime - iterStartTime
      
      // Get undecided count at the end of the iteration
      val undecidedCount = g.vertices.filter(_._2 == 0).count()
      
      // Store stats for this iteration
      iterationStats = iterationStats :+ (iterationCount, undecidedCount, iterDuration)
      
      // Print iteration stats - concise format for immediate feedback
      println(f"${iterationCount}%8d | ${undecidedCount}%19d | ${iterDuration}%8d")
    }
    
    // End time for the entire algorithm
    val totalEndTime = System.currentTimeMillis()
    val totalDuration = totalEndTime - totalStartTime
    
    // Print final detailed summary table
    println("\n============ FINAL SUMMARY ============")
    println(s"TOTAL VERTICES: $totalVertices")
    println(s"TOTAL EXECUTION TIME: $totalDuration ms")
    println(s"TOTAL ITERATIONS: $iterationCount")
    
    // Count vertices in MIS
    val verticesInMIS = g.vertices.filter(_._2 == 1).count()
    println(s"VERTICES IN MIS: $verticesInMIS (${(verticesInMIS.toDouble / totalVertices * 100).round}%)")
    
    // Print iteration details in the summary
    println("\nDETAILED ITERATION STATISTICS:")
    println("ITERATION | UNDECIDED VERTICES | TIME (ms)")
    println("----------|-------------------|----------")
    iterationStats.foreach { case (iter, undecided, time) =>
      println(f"${iter}%8d | ${undecided}%19d | ${time}%8d")
    }
    println("======================================")
    
    // Return the final graph with 1 (in MIS) or -1 (not in MIS)
    g
  }

  def verifyMIS(g_in: Graph[Int, Int]): Boolean = {
    // Check independence property: No two vertices in MIS should be adjacent
    val independenceViolations = g_in
      .triplets
      .filter(et => et.srcAttr == 1 && et.dstAttr == 1)
      .count()
    
    // If there are any violations of independence property, return false
    if (independenceViolations > 0) {
      println("Independence property violated!")
      return false
    }
    
    // For maximality check, first compute the degrees of all vertices
    val degrees = g_in.degrees
    
    // Identify isolated vertices (degree = 0)
    val isolatedVertices = degrees
      .filter(_._2 == 0)
      .map(_._1)
      .collect()
      .toSet
    
    // All isolated vertices must be in MIS for maximality
    val isolatedNonMisVertices = g_in.vertices
      .filter { case (vid, attr) => 
        isolatedVertices.contains(vid) && attr == -1 
      }
      .count()
    
    if (isolatedNonMisVertices > 0) {
      println("Maximality violated: isolated vertex not in MIS!")
      return false
    }
    
    // For non-isolated vertices outside MIS, collect info about neighbors in MIS
    val hasNeighborInMIS = g_in.aggregateMessages[Boolean](
      triplet => {
        // If destination is in MIS, send true to source
        if (triplet.dstAttr == 1) {
          triplet.sendToSrc(true)
        }
        // If source is in MIS, send true to destination
        if (triplet.srcAttr == 1) {
          triplet.sendToDst(true)
        }
      },
      // Combine messages - OR operation (true if any neighbor is in MIS)
      (a, b) => a || b,
      TripletFields.All
    )
    
    // Create a mappable join of vertices with their MIS neighbor information
    val verticesWithNeighborInfo = g_in.vertices
      .filter(_._2 == -1) // Only consider non-MIS vertices
      .leftOuterJoin(hasNeighborInMIS)
      .map { case (vid, (attr, optHasNeighbor)) => 
        (vid, (attr, optHasNeighbor.getOrElse(false)))
      }
    
    // Find non-MIS vertices that don't have any MIS neighbors and are not isolated
    val nonMisVerticesWithoutMisNeighbors = verticesWithNeighborInfo
      .filter { case (vid, (attr, hasNeighbor)) => 
        !hasNeighbor && !isolatedVertices.contains(vid)
      }
      .count()
    
    // If any non-isolated, non-MIS vertex has no neighbor in MIS, maximality is violated
    if (nonMisVerticesWithoutMisNeighbors > 0) {
      println("Maximality violated: non-MIS vertex with no MIS neighbors!")
      return false
    }
    
    // Both properties are satisfied
    return true
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("project_3")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()
    /* You can either use sc or spark */

    if(args.length == 0) {
      println("Usage: project_3 option = {compute, verify}")
      sys.exit(1)
    }
    if(args(0)=="compute") {
      if(args.length != 3) {
        println("Usage: project_3 compute graph_path output_path")
        sys.exit(1)
      }
      val startTimeMillis = System.currentTimeMillis()
      val edges = sc.textFile(args(1)).map(line => {val x = line.split(","); Edge(x(0).toLong, x(1).toLong , 1)} )
      val g = Graph.fromEdges[Int, Int](edges, 0, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)
      val g2 = LubyMIS(g)

      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
      println("==================================")
      println("Luby's algorithm completed in " + durationSeconds + "s.")
      println("==================================")

      val g2df = spark.createDataFrame(g2.vertices)
      g2df.coalesce(1).write.format("csv").mode("overwrite").save(args(2))
    }
    else if(args(0)=="verify") {
      if(args.length != 3) {
        println("Usage: project_3 verify graph_path MIS_path")
        sys.exit(1)
      }

      val edges = sc.textFile(args(1)).map(line => {val x = line.split(","); Edge(x(0).toLong, x(1).toLong , 1)} )
      val vertices = sc.textFile(args(2)).map(line => {val x = line.split(","); (x(0).toLong, x(1).toInt) })
      val g = Graph[Int, Int](vertices, edges, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)

      val ans = verifyMIS(g)
      if(ans)
        println("Yes")
      else
        println("No")
    }
    else
    {
        println("Usage: project_3 option = {compute, verify}")
        sys.exit(1)
    }
  }
}