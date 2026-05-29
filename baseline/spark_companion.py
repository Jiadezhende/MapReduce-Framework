#!/usr/bin/env python3
import argparse
import json
import xml.etree.ElementTree as ET

from pyspark.sql import SparkSession, functions as F, types as T


def load_config(path):
    tree = ET.parse(path)
    root = tree.getroot()
    conf = {}
    for prop in root.findall("property"):
        name = prop.findtext("name")
        value = prop.findtext("value")
        if name and value is not None:
            conf[name] = value.strip()
    return conf


def parse_args():
    parser = argparse.ArgumentParser(description="Spark baseline for companion pairs")
    parser.add_argument("--input", required=True, help="input CSV: vid,loc,ts")
    parser.add_argument("--output", required=True, help="output directory for vidA,vidB,count")
    parser.add_argument("--config", default="common/src/main/resources/companion-conf.xml")
    parser.add_argument("--t0", type=int)
    parser.add_argument("--delta-t", type=int)
    parser.add_argument("--k-min", type=int)
    parser.add_argument("--slot-size", type=int)
    parser.add_argument("--shuffle-partitions", type=int)
    parser.add_argument("--emit-config", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    conf = load_config(args.config)

    t0 = args.t0 if args.t0 is not None else int(conf.get("companion.t0", "1420041600"))
    delta_t = args.delta_t if args.delta_t is not None else int(conf.get("companion.delta.t", "300"))
    k_min = args.k_min if args.k_min is not None else int(conf.get("companion.k.min", "3"))
    slot_size = args.slot_size if args.slot_size is not None else int(conf.get("companion.slot.size", "300"))

    if args.emit_config:
        print(json.dumps({
            "t0": t0,
            "delta_t": delta_t,
            "k_min": k_min,
            "slot_size": slot_size
        }, indent=2))
        return

    spark = SparkSession.builder.appName("companion-baseline-spark").getOrCreate()
    if args.shuffle_partitions:
        spark.conf.set("spark.sql.shuffle.partitions", str(args.shuffle_partitions))

    schema = T.StructType([
        T.StructField("vid", T.LongType(), False),
        T.StructField("loc", T.LongType(), False),
        T.StructField("ts", T.LongType(), False),
    ])

    df = spark.read.csv(args.input, schema=schema)

    kept = df.groupBy("vid").count().filter(F.col("count") >= 2).select("vid")
    df = df.join(kept, "vid", "inner")

    df = df.withColumn("t_norm", F.col("ts") - F.lit(t0))
    df = df.withColumn("slot", F.floor(F.col("t_norm") / F.lit(slot_size)).cast("long"))

    a = df.alias("a")
    b = df.alias("b")
    cond = (
        (F.col("a.loc") == F.col("b.loc"))
        & (F.col("a.vid") < F.col("b.vid"))
        & (F.abs(F.col("a.t_norm") - F.col("b.t_norm")) <= F.lit(delta_t))
    )

    joined = a.join(b, cond, "inner")
    slot_w = F.when(F.col("a.t_norm") >= F.col("b.t_norm"), F.col("a.slot")).otherwise(F.col("b.slot"))

    witnesses = joined.select(
        F.col("a.vid").alias("vidA"),
        F.col("b.vid").alias("vidB"),
        F.col("a.loc").alias("loc"),
        slot_w.alias("slot"),
    ).dropDuplicates(["vidA", "vidB", "loc", "slot"])

    counts = witnesses.groupBy("vidA", "vidB").count()
    counts = counts.filter(F.col("count") >= F.lit(k_min))

    ordered = counts.orderBy(F.desc("count"), F.col("vidA"), F.col("vidB"))
    ordered.coalesce(1).write.mode("overwrite").option("header", "false").option("sep", ",").csv(args.output)

    spark.stop()


if __name__ == "__main__":
    main()
