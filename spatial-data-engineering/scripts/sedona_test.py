from pyspark.sql import SparkSession
from sedona.spark import SedonaContext


# --------------------------------------------------
# 1. Create Spark session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("GeoPluse-Sedona-Test")
    .master("local[2]")
    .config(
        "spark.jars.packages",
        "org.apache.sedona:sedona-spark-4.1_2.13:1.9.0,"
        "org.datasyslab:geotools-wrapper:1.9.0-33.5"
    )
    .getOrCreate()
)


# --------------------------------------------------
# 2. Create Sedona context
# --------------------------------------------------

sedona = SedonaContext.create(spark)

print("========================================")
print("Apache Sedona started successfully!")
print("Spark version:", spark.version)
print("========================================")


# --------------------------------------------------
# 3. Create one geographic point
# --------------------------------------------------

point_df = spark.sql("""
    SELECT ST_Point(73.8567, 18.5204) AS geometry
""")

print("\nOriginal Point:")
point_df.show(truncate=False)


# --------------------------------------------------
# 4. Create 500-meter catchment polygon
# --------------------------------------------------

buffer_df = spark.sql("""
    SELECT
        ST_Buffer(
            ST_Transform(
                ST_Point(73.8567, 18.5204),
                'epsg:4326',
                'epsg:32643'
            ),
            500
        ) AS catchment
""")

print("\n500-meter Catchment Polygon:")
buffer_df.show(truncate=False)


# --------------------------------------------------
# 5. Count generated polygons
# --------------------------------------------------

print(
    "\nNumber of catchment polygons:",
    buffer_df.count()
)


# --------------------------------------------------
# 6. Stop Spark
# --------------------------------------------------

spark.stop()

print("\n========================================")
print("Sedona test completed successfully!")
print("========================================")