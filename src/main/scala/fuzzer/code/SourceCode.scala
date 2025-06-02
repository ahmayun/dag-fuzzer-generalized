package fuzzer.code

case class SourceCode(src: String, ast: scala.meta.Tree) {
  override def toString: String = src
}
