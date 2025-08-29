package fuzzer.core.interfaces

import scala.collection.mutable.ListBuffer

case class ExecutionResult(
                            success: Boolean,
                            exception: Throwable,
                            combinedSourceWithResults: String = "",
                            coverage: ListBuffer[String] = ListBuffer.empty
                          )
