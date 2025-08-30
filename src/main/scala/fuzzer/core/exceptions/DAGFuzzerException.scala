package fuzzer.core.exceptions

class DAGFuzzerException(message: String, val inner: Exception) extends Exception(message)
