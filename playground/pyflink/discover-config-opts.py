from pyflink.java_gateway import get_gateway

gateway = get_gateway()

try:
    # Try to access optimizer config options
    optimizer_config = gateway.jvm.org.apache.flink.table.api.config.OptimizerConfigOptions
    print("OptimizerConfigOptions found!")
    print("Available optimizer fields:")
    optimizer_fields = [attr for attr in dir(optimizer_config) if not attr.startswith('_')]
    for field in optimizer_fields:
        print(field)

except Exception as e:
    print(f"Could not access OptimizerConfigOptions: {e}")

    # Let's also try ExecutionConfigOptions
    try:
        exec_config = gateway.jvm.org.apache.flink.table.api.config.ExecutionConfigOptions
        print("ExecutionConfigOptions found!")
        print("Available execution fields:")
        exec_fields = [attr for attr in dir(exec_config) if not attr.startswith('_')]
        for field in exec_fields:
            print(field)
    except Exception as e2:
        print(f"Could not access ExecutionConfigOptions: {e2}")