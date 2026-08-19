import os

from sedona.spark import SedonaContext


# --------------------------------------------------
# 1. Set Windows temporary directory
# --------------------------------------------------

os.environ["TEMP"] = os.path.join(
    os.environ["USERPROFILE"],
    "AppData",
    "Local",
    "Temp",
    "GeoPluseSpark"
)

os.environ["TMP"] = os.environ["TEMP"]

os.environ["SPARK_LOCAL_DIRS"] = os.environ["TEMP"]


# --------------------------------------------------
# 2. Create Sedona configuration
# --------------------------------------------------

config = (
    SedonaContext.builder()
    .master("local[2]")
    .appName("GeoPluse-Sedona-Test")
    .config(
        "spark.jars.packages",
        "org.apache.sedona:sedona-spark-4.1_2.13:1.9.0,"
        "org.datasyslab:geotools-wrapper:1.9.0-33.5"
    )
    .config(
        "spark.local.dir",
        os.environ["SPARK_LOCAL_DIRS"]
    )
    .getOrCreate()
)


# --------------------------------------------------
# 3. Create Sedona context
# --------------------------------------------------

sedona = SedonaContext.create(config)

print()
print("========================================")
print("Apache Sedona started successfully!")
print("Spark version:", sedona.version)
print("========================================")


# --------------------------------------------------
# 4. Test ST_Point
# --------------------------------------------------

point = sedona.sql("""
    SELECT ST_Point(73.8567, 18.5204) AS geometry
""")

print()
print("Original Point:")
point.show(truncate=False)


# --------------------------------------------------
# 5. Test 500-meter catchment area
# --------------------------------------------------

catchment = sedona.sql("""
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

print()
print("500-meter Catchment Polygon:")
catchment.show(truncate=False)


# --------------------------------------------------
# 6. Count polygons
# --------------------------------------------------

count = catchment.count()

print()
print("Number of catchment polygons:", count)


# --------------------------------------------------
# 7. Finish
# --------------------------------------------------

sedona.stop()

print()
print("========================================")
print("Sedona test completed successfully!")
print("========================================")