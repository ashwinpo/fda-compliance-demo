# Databricks notebook source
# MAGIC %md
# MAGIC # FDA Compliance — Declarative Pipeline
# MAGIC
# MAGIC Medallion pipeline ingesting openFDA data for regulatory compliance monitoring.
# MAGIC
# MAGIC **Sources:** NDC Product Directory · Food/Supplement Enforcement · Drug Adverse Events
# MAGIC
# MAGIC **Architecture:** UC Volume (raw JSON) → Bronze → Silver → Gold → Lakebase

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import *

VOLUME = spark.conf.get("volume_path")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze — Raw FDA Data

# COMMAND ----------

@dp.table(
    name="raw_ndc_products",
    comment="Raw FDA NDC product directory records",
    table_properties={"quality": "bronze"},
)
def raw_ndc_products():
    return (
        spark.read.option("multiLine", True).json(f"{VOLUME}/ndc_products.json")
        .withColumn("_ingestion_ts", F.current_timestamp())
        .withColumn("_source", F.lit("openFDA /drug/ndc.json"))
    )

# COMMAND ----------

@dp.table(
    name="raw_enforcement_actions",
    comment="Raw FDA food/supplement enforcement and recall records",
    table_properties={"quality": "bronze"},
)
def raw_enforcement_actions():
    return (
        spark.read.option("multiLine", True).json(f"{VOLUME}/enforcement_actions.json")
        .withColumn("_ingestion_ts", F.current_timestamp())
        .withColumn("_source", F.lit("openFDA /food/enforcement.json"))
    )

# COMMAND ----------

@dp.table(
    name="raw_adverse_events",
    comment="Raw FDA drug adverse event reports",
    table_properties={"quality": "bronze"},
)
def raw_adverse_events():
    return (
        spark.read.option("multiLine", True).json(f"{VOLUME}/adverse_events.json")
        .withColumn("_ingestion_ts", F.current_timestamp())
        .withColumn("_source", F.lit("openFDA /drug/event.json"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver — Cleaned & Flattened

# COMMAND ----------

@dp.table(
    name="clean_products",
    comment="Cleaned product catalog with flattened ingredients and routes",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("has_ndc", "product_ndc IS NOT NULL")
@dp.expect("has_brand", "brand_name IS NOT NULL")
def clean_products():
    return (
        spark.read.table("raw_ndc_products")
        .select(
            F.trim(F.col("product_ndc")).alias("product_ndc"),
            F.upper(F.trim(F.col("brand_name"))).alias("brand_name"),
            F.upper(F.trim(F.col("generic_name"))).alias("generic_name"),
            F.trim(F.col("labeler_name")).alias("labeler_name"),
            F.col("product_type"),
            F.col("dosage_form"),
            F.col("marketing_category"),
            F.col("route").getItem(0).alias("primary_route"),
            F.col("active_ingredients").getItem(0).getField("name").alias("primary_ingredient"),
            F.col("active_ingredients").getItem(0).getField("strength").alias("primary_strength"),
            F.size(F.col("active_ingredients")).alias("ingredient_count"),
        )
        .dropDuplicates(["product_ndc"])
    )

# COMMAND ----------

@dp.table(
    name="clean_enforcement",
    comment="Cleaned enforcement actions with parsed dates and classification",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("has_recall_number", "recall_number IS NOT NULL")
@dp.expect("valid_classification", "classification IN ('Class I', 'Class II', 'Class III')")
def clean_enforcement():
    return (
        spark.read.table("raw_enforcement_actions")
        .select(
            F.trim(F.col("recall_number")).alias("recall_number"),
            F.col("classification"),
            F.col("status"),
            F.col("product_description"),
            F.col("reason_for_recall"),
            F.trim(F.col("recalling_firm")).alias("recalling_firm"),
            F.col("city"),
            F.col("state"),
            F.col("country"),
            F.col("distribution_pattern"),
            F.col("voluntary_mandated"),
            F.to_date(F.col("recall_initiation_date"), "yyyyMMdd").alias("recall_date"),
            F.to_date(F.col("report_date"), "yyyyMMdd").alias("report_date"),
            F.to_date(F.col("termination_date"), "yyyyMMdd").alias("termination_date"),
        )
        .dropDuplicates(["recall_number"])
    )

# COMMAND ----------

@dp.table(
    name="clean_adverse_events",
    comment="Flattened adverse event reports with one row per drug-reaction pair",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("has_report_id", "report_id IS NOT NULL")
def clean_adverse_events():
    return (
        spark.read.table("raw_adverse_events")
        .select(
            F.col("safetyreportid").alias("report_id"),
            F.col("serious").cast("int").alias("is_serious"),
            F.to_date(F.col("receivedate"), "yyyyMMdd").alias("receive_date"),
            F.col("patient.patientsex").cast("int").alias("patient_sex"),
            F.col("patient.patientonsetage").cast("int").alias("patient_age"),
            F.col("patient.drug").alias("drugs"),
            F.col("patient.reaction").alias("reactions"),
        )
        .withColumn("drug", F.explode_outer("drugs"))
        .withColumn("product_name", F.upper(F.trim(F.col("drug.medicinalproduct"))))
        .withColumn("drug_indication", F.col("drug.drugindication"))
        .withColumn("reaction", F.explode_outer("reactions"))
        .withColumn("reaction_name", F.upper(F.trim(F.col("reaction.reactionmeddrapt"))))
        .drop("drugs", "reactions", "drug", "reaction")
        .filter(F.col("product_name").isNotNull())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold — Analytics-Ready (CDF enabled for Lakebase sync)

# COMMAND ----------

@dp.table(
    name="gold_product_catalog",
    comment="Deduplicated product reference table — Lakebase sync candidate (PK: product_ndc)",
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true",
    },
)
def gold_product_catalog():
    return spark.read.table("clean_products")

# COMMAND ----------

@dp.table(
    name="gold_enforcement_actions",
    comment="Clean enforcement/recall table for compliance monitoring (PK: recall_number)",
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true",
    },
)
def gold_enforcement_actions():
    return spark.read.table("clean_enforcement")

# COMMAND ----------

@dp.table(
    name="gold_adverse_events_summary",
    comment="Adverse event counts by product and reaction type",
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true",
    },
)
def gold_adverse_events_summary():
    return (
        spark.read.table("clean_adverse_events")
        .groupBy("product_name", "reaction_name")
        .agg(
            F.count("*").alias("event_count"),
            F.sum(F.when(F.col("is_serious") == 1, 1).otherwise(0)).alias("serious_count"),
            F.min("receive_date").alias("earliest_report"),
            F.max("receive_date").alias("latest_report"),
        )
        .filter(F.col("product_name").isNotNull() & F.col("reaction_name").isNotNull())
    )
