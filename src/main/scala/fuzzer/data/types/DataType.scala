package fuzzer.data.types

import fuzzer.utils.random.Random


trait DataType {
  def name: String
}

object DataType {
  def generateRandom: DataType = {
    val l = Array(
      BooleanType,
      DateType,
      DecimalType,
      FloatType,
      IntegerType,
      LongType,
      StringType
    )

    l(Random.nextInt(l.length))
  }
}