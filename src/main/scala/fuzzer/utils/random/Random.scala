package fuzzer.utils.random

object Random {
  private var rng = new scala.util.Random(
    fuzzer.core.global.State.config match {
      case Some(config) => config.seed
      case _ => 0
    }
  ) // default seed

  def setSeed(seed: Long): Unit = {
    rng = new scala.util.Random(seed)
    scala.util.Random.setSeed(seed)
  }

  def nextInt(): Int = rng.nextInt()
  def nextInt(n: Int): Int = rng.nextInt(n)
  def nextIntInclusiveRange(low: Int, high: Int): Int = rng.nextInt(high - low + 1) + low
  def nextDouble(): Double = rng.nextDouble()
  def nextBoolean(): Boolean = rng.nextBoolean()
  def nextFloat(): Float = rng.nextFloat()
  def nextLong(): Long = rng.nextLong()
  def nextBytes(bytes: Array[Byte]): Unit = rng.nextBytes(bytes)
  def shuffle[T](xs: Seq[T]): Seq[T] = rng.shuffle(xs)
  def nextGaussian(): Double = rng.nextGaussian()
  def alphanumeric: LazyList[Char] = rng.alphanumeric
}

