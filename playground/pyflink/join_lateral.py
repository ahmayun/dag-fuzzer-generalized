from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes
from pyflink.table.expressions import col
from pyflink.table.udf import udtf

# Setup environment
env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_settings)

# Create sample data - employees table
employees_data = [
    (1, "Alice", "Python,Java,SQL"),
    (2, "Bob", "Java,Scala"),
    (3, "Charlie", "Python,C++"),
]

employees = table_env.from_elements(
    employees_data,
    schema=['emp_id', 'emp_name', 'skills']
)

# Define a Table Function that splits skills
@udtf(result_types=[DataTypes.STRING()])
def split_skills(skills_str):
    if skills_str:
        for skill in skills_str.split(','):
            yield skill.strip(),

# Use join_lateral with proper syntax
result = employees.join_lateral(
    split_skills(col('skills')).alias('skill')
)

# Execute and print
result.select(col('emp_id'), col('emp_name'), col('skill')).execute().print()