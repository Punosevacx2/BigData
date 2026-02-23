import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

def save_top_by_value(df, key: str, out_path: str, n: int = 30) -> None:
    """
    Filtrira redove gde je k == key, pa broji po v (value).
    Snima rezultat kao CSV (jedan part fajl).
    """
    (df.filter(col("k") == key)
       .groupBy("v")
       .agg(count("*").alias("cnt"))
       .orderBy(col("cnt").desc())
       .limit(n)
       .coalesce(1)
       .write.mode("overwrite")
       .csv(out_path, header=True)
    )

def main(parquet_path: str, out_dir: str) -> None:
    spark = SparkSession.builder.appName("OSM_Infra_Analysis").getOrCreate()

    if not parquet_path.startswith("hdfs://") and not os.path.exists(parquet_path):
        raise FileNotFoundError(f"Parquet path does not exist: {parquet_path}")

    os.makedirs(out_dir, exist_ok=True)

    df = spark.read.parquet(parquet_path).cache()

    # Core analize
    save_top_by_value(df, "highway", os.path.join(out_dir, "top_highway"), n=30)
    save_top_by_value(df, "amenity", os.path.join(out_dir, "top_amenity"), n=30)

    # Ukupan broj zgrada (building)
    buildings_count = df.filter(col("k") == "building").count()
    with open(os.path.join(out_dir, "buildings_count.txt"), "w", encoding="utf-8") as f:
        f.write(str(buildings_count))

    # Bonus analize (ako postoje tagovi, dobićeš fajl; ako ne postoje, CSV će biti prazan)
    for key, n in [
        ("oneway", 20),
        ("lanes", 20),
        ("maxspeed", 20),
        ("surface", 20),
        ("bridge", 20),
        ("railway", 20),
        ("public_transport", 20),
        ("landuse", 20),
    ]:
        save_top_by_value(df, key, os.path.join(out_dir, f"top_{key}"), n=n)

    spark.stop()
    print("Done. Results saved to:", out_dir)

if __name__ == "__main__":
    # usage: python src/analyze.py data/processed/osm.parquet results
    if len(sys.argv) != 3:
        print("Usage: python src/analyze.py <input.parquet_dir> <output_results_dir>")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])
