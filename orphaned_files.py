from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Find Orphan Files").getOrCreate()

# External location
external_location = "/your/external/location/path"

# 1. Get paths of all tables in Unity catalog
table_paths = []
databases = [db['databaseName'] for db in spark.sql("SHOW DATABASES").collect()]
for db in databases:
    for tbl in spark.sql(f"SHOW TABLES IN {db}").collect():
        table_name = tbl['tableName']
        table_path = spark.sql(f"DESCRIBE EXTENDED {db}.{table_name}").where("col_name = 'Location'").collect()[0]['data_type']
        table_paths.append(table_path)

# 2. List all files in the external location
all_files = [f.path for f in dbutils.fs.ls(external_location)]

# 3. Find orphaned files
orphan_files = [f for f in all_files if not any(f.startswith(table_path) for table_path in table_paths)]

print(orphan_files)
