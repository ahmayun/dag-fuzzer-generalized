import tensorflow as tf
import numpy as np

def reduce_udf(state, *x):
    # Example reduce: sum accumulator
    print("reducing")
    return state + x[0]

def scan_udf(state, *x):
    # Example scan: cumulative sum
    print("scanning")
    new_state = state + x[0]
    return new_state, new_state

autonode_3 = tf.data.Dataset.range(9, 87)
autonode_2 = autonode_3.repeat()
autonode_1 = autonode_2.scan(initial_state=tf.constant(0, dtype=tf.int64), scan_func=scan_udf)
sink = autonode_1.reduce(initial_state=tf.constant(0, dtype=tf.int64), reduce_func=reduce_udf)
print(sink)