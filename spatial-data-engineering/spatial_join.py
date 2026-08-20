import os

from pyspark.sql import functions as F
from sedona.spark import SedonaContext


# ============================================================
# 1. Windows temporary directory
# ============================================================

temp_dir = os.path.join(
    os.environ["USERPROFILE"],
    "AppData",
    "Local",
    "Temp",
    "GeoPluseSpark"
)

os.makedirs(temp_dir, exist_ok=True)

os.environ["TEMP"] = temp_dir
os.environ["TMP"] = temp_dir
os.environ["SPARK_LOCAL_DIRS"] = temp_dir


# ============================================================
# 2. Start Spark + Apache Sedona
# ============================================================

spark = (
    SedonaContext.builder()
    .master("local[2]")
    .appName("GeoPluse-Week2-Spatial-Join")
    .config("spark.local.dir", temp_dir)
    .getOrCreate()
)

sedona = SedonaContext.create(spark)

print()
print("==============================================")
print("WEEK 2 - SPATIAL JOIN")
print("==============================================")
print("Spark version:", spark.version)


# ============================================================
# 3. File paths
# ============================================================

stores_path = "spatial-data-engineering/data/stores.csv"

pings_path = "spatial-data-engineering/data/mobile_pings.csv"


# ============================================================
# 4. Read stores.csv
# ============================================================

stores_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(stores_path)
)

print()
print("STORE DATA:")
stores_df.show(10, truncate=False)

print("Number of stores:", stores_df.count())


# ============================================================
# 5. Read mobile_pings.csv
# ============================================================

pings_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(pings_path)
)

print()
print("MOBILE PING DATA:")
pings_df.show(10, truncate=False)

print("Number of mobile pings:", pings_df.count())


# ============================================================
# 6. Create POINT geometry for stores
#
# Longitude = X
# Latitude  = Y
#
# EPSG:4326 = normal GPS coordinates
# ============================================================

stores_points = stores_df.withColumn(
    "store_point",
    F.expr(
        "ST_Point(CAST(Longitude AS DOUBLE), CAST(Latitude AS DOUBLE))"
    )
)

print()
print("STORE POINT GEOMETRY CREATED")


# ============================================================
# 7. Convert store points from WGS84 to UTM
#
# EPSG:32643 = UTM Zone 43N
# Suitable for Pune/Maharashtra area
# Units are meters.
# ============================================================

stores_projected = stores_points.withColumn(
    "store_utm",
    F.expr(
        "ST_Transform(store_point, 'epsg:4326', 'epsg:32643')"
    )
)


# ============================================================
# 8. Create 500-meter catchment area
# ============================================================

stores_catchment = stores_projected.withColumn(
    "catchment_500m",
    F.expr(
        "ST_Buffer(store_utm, 500)"
    )
)

print()
print("500-METER CATCHMENT POLYGONS CREATED")


# ============================================================
# 9. Create POINT geometry for mobile pings
# ============================================================

pings_points = pings_df.withColumn(
    "ping_point",
    F.expr(
        "ST_Point(CAST(Longitude AS DOUBLE), CAST(Latitude AS DOUBLE))"
    )
)


# ============================================================
# 10. Convert mobile ping points to UTM
# ============================================================

pings_projected = pings_points.withColumn(
    "ping_utm",
    F.expr(
        "ST_Transform(ping_point, 'epsg:4326', 'epsg:32643')"
    )
)

print()
print("MOBILE PING POINT GEOMETRY CREATED")


# ============================================================
# 11. Create temporary SQL views
# ============================================================

stores_catchment.createOrReplaceTempView("stores_catchment")

pings_projected.createOrReplaceTempView("mobile_pings")


# ============================================================
# 12. SPATIAL JOIN
#
# Find mobile pings that fall inside the
# 500-meter catchment of each store.
# ============================================================

spatial_join = spark.sql("""
    SELECT
        s.StoreID,
        s.StoreName,
        s.Latitude AS StoreLatitude,
        s.Longitude AS StoreLongitude,

        p.DeviceID,
        p.Latitude AS PingLatitude,
        p.Longitude AS PingLongitude,
        p.Timestamp

    FROM stores_catchment s

    INNER JOIN mobile_pings p

        ON ST_Contains(
            s.catchment_500m,
            p.ping_utm
        )
""")


# ============================================================
# 13. Display spatial join results
# ============================================================

print()
print("==============================================")
print("SPATIAL JOIN RESULTS")
print("==============================================")

spatial_join.show(30, truncate=False)


# ============================================================
# 14. Count matched pings
# ============================================================

matched_count = spatial_join.count()

print()
print("Number of pings inside store catchments:", matched_count)


# ============================================================
# 15. Count visitors by store
# ============================================================

visitor_summary = (
    spatial_join
    .groupBy("StoreID", "StoreName")
    .agg(
        F.countDistinct("DeviceID").alias("UniqueVisitors")
    )
    .orderBy("StoreID")
)

print()
print("==============================================")
print("UNIQUE VISITORS BY STORE")
print("==============================================")

visitor_summary.show(truncate=False)


# ============================================================
# 16. Save spatial join result
# ============================================================

output_path = "spatial-data-engineering/data/spatial_join_results"

(
    spatial_join
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(output_path)
)

print()
print("Spatial join results saved to:")
print(output_path)


# ============================================================
# 17. Stop Spark
# ============================================================

sedona.stop()

print()
print("==============================================")
print("WEEK 2 SPATIAL JOIN COMPLETED")
print("==============================================")