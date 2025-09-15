from pyflink.java_gateway import get_gateway

gateway = get_gateway()
optimizer_config = gateway.jvm.org.apache.flink.table.api.config.OptimizerConfigOptions

# Get the actual configuration key strings
print("Available optimizer configuration keys:")
print("="*50)

optimizer_fields = [
    'TABLE_OPTIMIZER_ADAPTIVE_BROADCAST_JOIN_STRATEGY',
    'TABLE_OPTIMIZER_ADAPTIVE_SKEWED_JOIN_OPTIMIZATION_STRATEGY',
    'TABLE_OPTIMIZER_AGG_PHASE_STRATEGY',
    'TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD',
    'TABLE_OPTIMIZER_BUSHY_JOIN_REORDER_THRESHOLD',
    'TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED',
    'TABLE_OPTIMIZER_DYNAMIC_FILTERING_ENABLED',
    'TABLE_OPTIMIZER_INCREMENTAL_AGG_ENABLED',
    'TABLE_OPTIMIZER_JOIN_REORDER_ENABLED',
    'TABLE_OPTIMIZER_MULTIPLE_INPUT_ENABLED',
    'TABLE_OPTIMIZER_REUSE_SOURCE_ENABLED',
    'TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED',
    'TABLE_OPTIMIZER_RUNTIME_FILTER_ENABLED',
    'TABLE_OPTIMIZER_SOURCE_REPORT_STATISTICS_ENABLED',
    'TABLE_OPTIMIZER_SQL2REL_PROJECT_MERGE_ENABLED',
    'TABLE_OPTIMIZER_UNIONALL_AS_BREAKPOINT_ENABLED'
]

for field_name in optimizer_fields:
    try:
        config_option = getattr(optimizer_config, field_name)
        key = config_option.key()
        print(f"{key}")
    except Exception as e:
        print(f"Error getting key for {field_name}: {e}")