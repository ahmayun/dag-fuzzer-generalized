package fuzzer.utils.generation.dag

import fuzzer.core.global.FuzzerConfig
import fuzzer.core.graph.{DAGParser, DFOperator, Graph, Node}
import fuzzer.utils.random.Random
import org.yaml.snakeyaml.Yaml

import java.io.{File, FileWriter}
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.sys.process._

object DAGGenUtils {

  def generateSingleDAG(config: FuzzerConfig): Graph[DFOperator] = {
    val dagYamlFile = generateYamlFiles(1, config.d, config.dagGenDir).head
    DAGParser.parseYamlFile(dagYamlFile.getAbsolutePath, map => DFOperator.fromMap(map))
  }

  def genTempConfigWithNewSeed(yamlFile: String): String = {
    val yaml = new Yaml()
    val newSeed = Random.nextInt(Int.MaxValue)
    val inputStream = new java.io.FileInputStream(new File(yamlFile))
    val data = yaml.load[java.util.Map[String, Object]](inputStream).asScala
    data.update("Seed", Integer.valueOf(newSeed))
    val outputPath = "/tmp/runtime-config.yaml"
    val writer = new FileWriter(outputPath)
    yaml.dump(data.asJava, writer)
    writer.close()
    outputPath
  }

  def updateConfigProperty(configFile: String, propName: String, propValue: Int): Unit = {
    val yaml = new Yaml()
    val inputStream = new java.io.FileInputStream(new File(configFile))
    val data = yaml.load[java.util.Map[String, Object]](inputStream).asScala
    data.update(propName, Integer.valueOf(propValue))
    val outputPath = "/tmp/runtime-config.yaml"
    val writer = new FileWriter(outputPath)
    yaml.dump(data.asJava, writer)
    writer.close()
  }

  def generateDAGFolder(n: Int, dagGenDir: String): File = {
    val newConfig = genTempConfigWithNewSeed("dag-gen/sample_config/dfg-config.yaml")
    updateConfigProperty(newConfig, "Number of DAGs", n)
    val generateCmd = s"./dag-gen/venv/bin/python dag-gen/run_generator.py -c ${newConfig}" // dag-gen/sample_config/dfg-config.yaml
    val exitCode = generateCmd.!
    if (exitCode != 0) {
      println(s"Warning: DAG generation command failed with exit code $exitCode")
      sys.exit(-1)
    }

    val dagFolder = new File(dagGenDir)
    if (!dagFolder.exists() || !dagFolder.isDirectory) {
      println("Warning: 'DAGs' folder not found or not a directory. Exiting.")
      sys.exit(-1)
    }
    dagFolder
  }

  def generateYamlFiles(n: Int, d: Int, dagGenDir: String): Array[File] = {

    val dagFolder = generateDAGFolder(n, dagGenDir)

    val yamlFiles = dagFolder
      .listFiles()
      .filter(f => f.isFile && f.getName.startsWith("dag") && f.getName.endsWith(".yaml"))
      .take(d)

    yamlFiles
  }

  def generateYamlFilesInfinite(n: Int, d: Int, dagGenDir: String): Iterator[(Graph[DFOperator], String)] = {
    var currentBatch: Iterator[File] = Iterator.empty
    var batchCount = 0

    def nextBatch(): Unit = {
      val dagFolder = generateDAGFolder(n, dagGenDir)
      val yamlFiles = dagFolder
        .listFiles()
        .filter(f => f.isFile && f.getName.startsWith("dag") && f.getName.endsWith(".yaml"))
        .take(d)

      currentBatch = yamlFiles.iterator
      batchCount += 1
    }

    new Iterator[(Graph[DFOperator], String)] {
      def hasNext: Boolean = {
        if (!currentBatch.hasNext) {
          nextBatch()
        }
        currentBatch.hasNext
      }

      def next(): (Graph[DFOperator], String) = {
        if (!hasNext) throw new NoSuchElementException("No more elements")
        val file = currentBatch.next()
        val dag = DAGParser.parseYamlFile(file.getAbsolutePath, map => DFOperator.fromMap(map))
        (dag, file.getName)
      }
    }
  }


  /**
   * Generates a random DAG that forms an inverted binary tree structure.
   * Creates a regular binary tree by splitting nodes, then reverses edges.
   * This guarantees no node has in-degree > 2 and creates a clean structure.
   *
   * @param valueGenerator A function that generates values for nodes given their level
   * @param splittingProbability The probability (0.0 to 1.0) that a node will split into children
   * @param twoChildProbability The probability (0.0 to 1.0) that a splitting node creates 2 children vs 1
   * @param maxDepth Maximum depth to prevent infinite expansion
   * @tparam T The type of values stored in nodes
   * @return A Graph representing an inverted binary tree DAG
   */
  def generateRandomInvertedBinaryTreeDAG[T](
                                              valueGenerator: Int => T,
                                              maxDepth: Int = 6,
                                              splittingProbability: Double = 1.0,
                                              twoChildProbability: Double = 0.3
                                            ): Graph[T] = {
    require(splittingProbability >= 0.0 && splittingProbability <= 1.0, "Splitting probability must be between 0 and 1")
    require(twoChildProbability >= 0.0 && twoChildProbability <= 1.0, "Two child probability must be between 0 and 1")
    require(maxDepth >= 1, "Max depth must be at least 1")

    val nodesMap = mutable.Map[String, Node[T]]()
    val childrenMap = mutable.Map[String, mutable.ListBuffer[String]]()
    val parentsMap = mutable.Map[String, mutable.ListBuffer[String]]()

    var nodeCounter = 0

    def createNode(level: Int): Node[T] = {
      val id = s"node_$nodeCounter"
      nodeCounter += 1
      val node = Node(id, valueGenerator(level))
      nodesMap(id) = node
      childrenMap(id) = mutable.ListBuffer.empty
      parentsMap(id) = mutable.ListBuffer.empty
      node
    }

    def addForwardEdge(parent: Node[T], child: Node[T]): Unit = {
      childrenMap(parent.id) += child.id
      parentsMap(child.id) += parent.id
    }

    // Create root node
    val root = createNode(0)
    val nodesToProcess = mutable.Queue[(Node[T], Int)]()
    nodesToProcess.enqueue((root, 0))

    // Build regular binary tree by splitting nodes
    while (nodesToProcess.nonEmpty) {
      val (currentNode, depth) = nodesToProcess.dequeue()

      // Decide if this node should split (have children)
      if (depth < maxDepth && Random.nextDouble() < splittingProbability) {
        // Decide number of children (1 or 2)
        val numChildren = if (Random.nextDouble() < twoChildProbability) 2 else 1

        // Create children
        for (_ <- 0 until numChildren) {
          val child = createNode(depth + 1)
          addForwardEdge(currentNode, child)
          nodesToProcess.enqueue((child, depth + 1))
        }
      }
    }

    // Now reverse all edges to create inverted tree
    val finalChildrenMap = mutable.Map[String, List[String]]()
    val finalParentsMap = mutable.Map[String, List[String]]()

    // Initialize all nodes with empty lists
    nodesMap.keys.foreach { nodeId =>
      finalChildrenMap(nodeId) = List.empty
      finalParentsMap(nodeId) = List.empty
    }

    // Reverse the edges: parent->child becomes child->parent
    for ((parentId, children) <- childrenMap) {
      for (childId <- children) {
        // Original: parent -> child
        // Reversed: child -> parent (child becomes parent, parent becomes child)
        finalChildrenMap(childId) = finalChildrenMap(childId) :+ parentId
        finalParentsMap(parentId) = finalParentsMap(parentId) :+ childId
      }
    }

    // Convert to immutable maps
    val finalNodesMap = nodesMap.toMap
    val finalChildrenMapImmutable = finalChildrenMap.toMap
    val finalParentsMapImmutable = finalParentsMap.toMap

    // Create the graph
    val graph = Graph(finalNodesMap, finalChildrenMapImmutable, finalParentsMapImmutable)

    // Set the graph reference for all nodes
    graph.nodes.foreach(_.graph = graph)

    graph
  }
}
