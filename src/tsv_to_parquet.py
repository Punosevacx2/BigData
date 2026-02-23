import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, when

def tsv_to_parquet(in_tsv: str, out_parquet: str) -> None:
    if not os.path.isfile(in_tsv):
        raise FileNotFoundError(f"TSV file not found: {in_tsv}")

    spark = (
        SparkSession.builder
        .appName("TSV_to_Parquet")
        # malo više memorije nego default (pomaže na Mac-u)
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )

    df = (
        spark.read
        .option("header", True)
        .option("sep", "\t")
        .option("quote", "\"")
        .option("escape", "\"")
        .option("multiLine", False)
        .option("mode", "DROPMALFORMED")  # izbaci pokvarene redove
        .csv(in_tsv)
    )

    # Očisti id od navodnika i whitespace-a
    df = df.withColumn("id_clean", regexp_replace(col("id"), r'["\s]', ""))

    # Safe cast: samo ako je čisto numeričko -> long, inače NULL
    df = df.withColumn(
        "id",
        when(col("id_clean").rlike(r"^\d+$"), col("id_clean").cast("long")).otherwise(None)
    ).drop("id_clean")

    # lat/lon safe cast
    df = df.withColumn(
        "lat",
        when(col("lat").rlike(r"^-?\d+(\.\d+)?$"), col("lat").cast("double")).otherwise(None)
    )
    df = df.withColumn(
        "lon",
        when(col("lon").rlike(r"^-?\d+(\.\d+)?$"), col("lon").cast("double")).otherwise(None)
    )

    os.makedirs(os.path.dirname(out_parquet), exist_ok=True)

    # izbaci redove gde id nije validan (parsing se polomio)
    df = df.filter(col("id").isNotNull())

    df.write.mode("overwrite").parquet(out_parquet)

    print(f"Done. Parquet written to: {out_parquet}")
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python src/tsv_to_parquet.py <input.tsv> <output.parquet_dir>")
        sys.exit(1)
    tsv_to_parquet(sys.argv[1], sys.argv[2])
