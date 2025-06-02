package fuzzer.core.interfaces

case class ExecutionResult(
                            success: Boolean,
                            output: Option[String] = None,
                            error: Option[Throwable] = None,
                            metrics: Map[String, Any] = Map.empty
                          )
