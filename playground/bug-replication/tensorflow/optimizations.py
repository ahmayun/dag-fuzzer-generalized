# ======== Program ========
import tensorflow as tf
import numpy as np
import timeit
import contextlib

# ----------------------------
# UDFs and helpers
# ----------------------------
def filter_udf(*x):
    return x[0] > 0

def map_udf(*x):
    return x[0] * 2

def flat_map_udf(*x):
    return tf.data.Dataset.from_tensors(x[0]).repeat(2)

def reduce_udf(state, *x):
    return state + x[0]

def scan_udf(state, *x):
    new_state = state + x[0]
    return new_state, new_state

def load_tf_csv(path, batch_size=32, skip_header=True):
    with tf.io.gfile.GFile(path, "r") as f:
        num_columns = len(f.readline().strip().split(","))
    record_defaults = [tf.constant("", tf.string)] * num_columns
    ds = tf.data.TextLineDataset(path)
    if skip_header:
        ds = ds.skip(1)
    ds = ds.map(lambda line: tuple(tf.io.decode_csv(line, record_defaults)))
    return ds.batch(batch_size).prefetch(tf.data.AUTOTUNE)

# ----------------------------
# 1) Generic: define your pipeline here
#    (Change only this function to try a new pipeline)
# ----------------------------
def build_pipeline():
    autonode_9 = tf.data.Dataset.range(4, 42)
    autonode_8 = tf.data.Dataset.range(2, 96)
    autonode_7 = tf.data.Dataset.range(1, 76)

    autonode_6 = autonode_9.prefetch(buffer_size=tf.data.AUTOTUNE)
    autonode_5 = autonode_7.concatenate(autonode_8)
    autonode_4 = autonode_6.scan(
        initial_state=tf.constant(0, dtype=tf.int64),
        scan_func=scan_udf,
    )
    autonode_3 = autonode_5.apply(transformation_func=lambda ds: ds)
    autonode_2 = autonode_4.shuffle(buffer_size=583)
    autonode_1 = autonode_3.scan(
        initial_state=tf.constant(0, dtype=tf.int64),
        scan_func=scan_udf,
    )
    sink = autonode_1.concatenate(autonode_2)
    return sink

# ----------------------------
# 2) Context manager to toggle Grappler options (from TF guide)
# ----------------------------
@contextlib.contextmanager
def optimizer_options(opts):
    old_opts = tf.config.optimizer.get_experimental_options()
    tf.config.optimizer.set_experimental_options(opts)
    try:
        yield
    finally:
        tf.config.optimizer.set_experimental_options(old_opts)

# ----------------------------
# 3) Generic graph consumer
#    - builds the pipeline inside tf.function
#    - consumes the whole dataset and returns a scalar
# ----------------------------
def _element_sum(elem):
    # Handles scalars, tensors, tuples, dicts, etc.
    flat = tf.nest.flatten(elem)
    parts = [tf.cast(tf.reduce_sum(e), tf.int64) for e in flat]
    return tf.add_n(parts)

def make_graph_runner():
    @tf.function
    def run_dataset():
        ds = build_pipeline()
        total = tf.constant(0, dtype=tf.int64)
        for elem in ds:
            total += _element_sum(elem)
        return total
    return run_dataset

# ----------------------------
# 4) Utility to run one variant (optimized / unoptimized)
# ----------------------------
def run_variant(label, optimizer_flags):
    with optimizer_options(optimizer_flags):
        print(f"\n=== {label} ===")
        print("Optimizer options:",
              tf.config.optimizer.get_experimental_options())

        # Build and trace the graph under this optimizer config
        run_ds = make_graph_runner()

        # Warm-up trace (ensure tracing cost is not in timing)
        _ = run_ds()

        # Time a single execution
        t = timeit.timeit(lambda: run_ds().numpy(), number=1)
        result = run_ds().numpy()
        print("Result:", result)
        print("Time:", t, "seconds")

# ----------------------------
# 5) Eager baseline: first N elements (just to sanity-check)
# ----------------------------
print("Eager baseline (first 10 elements):")
for e in build_pipeline().take(10):
    print(e)

# ----------------------------
# 6) Run unoptimized vs optimized
# ----------------------------
# Unoptimized: disable the meta-optimizer (turns off Grappler passes)
run_variant(
    label="Graph execution with Grappler DISABLED",
    optimizer_flags={"disable_meta_optimizer": True},
)

# Optimized: enable Grappler (default behavior)
run_variant(
    label="Graph execution with Grappler ENABLED",
    optimizer_flags={"disable_meta_optimizer": False},
)
