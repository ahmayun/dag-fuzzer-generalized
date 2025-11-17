package fuzzer.code

case class SourceCode(src: String, ast: scala.meta.Tree, preamble: String = "") {
  override def toString: String = src
}
