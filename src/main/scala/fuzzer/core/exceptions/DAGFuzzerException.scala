package fuzzer.core.exceptions

class DAGFuzzerException(message: String, val inner: Throwable) extends Exception(message)
