package misc.frameworks.flink

//package misc.frameworks.flink
//
//object BasicExample {
//  def main(args: Array[String]): Unit = {
//    // Set up the execution environment
//    val env = ExecutionEnvironment.getExecutionEnvironment
//
//    // Create a DataSet
//    val data = env.fromElements("apple", "banana", "cherry", "apple", "banana")
//
//    // Do some transformations (e.g., count the words)
//    val wordCounts = data
//      .map(word => (word, 1))
//      .groupBy(0)
//      .sum(1)
//
//    // Print the results
//    wordCounts.print()
//  }
//}