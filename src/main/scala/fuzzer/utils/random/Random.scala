package fuzzer.utils.random

import fuzzer.core.global.FuzzerConfig

import scala.util.Random

object Random {
  private var rng = new Random(fuzzer.core.global.State.config.get.seed) // default seed

  def setSeed(seed: Long): Unit = {
    rng = new Random(seed)
    scala.util.Random.setSeed(seed)
  }

  def nextInt(): Int = rng.nextInt()
  def nextInt(n: Int): Int = rng.nextInt(n)
  def nextDouble(): Double = rng.nextDouble()
  def nextBoolean(): Boolean = rng.nextBoolean()
  def nextFloat(): Float = rng.nextFloat()
  def nextLong(): Long = rng.nextLong()
  def nextBytes(bytes: Array[Byte]): Unit = rng.nextBytes(bytes)
  def shuffle[T](xs: Seq[T]): Seq[T] = rng.shuffle(xs)
  def nextGaussian(): Double = rng.nextGaussian()
  def alphanumeric: LazyList[Char] = rng.alphanumeric
}

