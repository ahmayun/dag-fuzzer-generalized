package fuzzer.core.interfaces

import fuzzer.code.SourceCode

trait CodeExecutor {
  def execute(code: SourceCode): ExecutionResult
  def setupEnvironment(): () => Unit
  def tearDownEnvironment(terminateF: () => Unit): Unit
}
