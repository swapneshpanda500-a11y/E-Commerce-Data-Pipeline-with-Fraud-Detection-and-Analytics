from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from pyspark.sql.functions import *
import datetime
class transformations:
    def dedup(self, df, dedup_cols, order_by_col):
        """
        Keep latest 1 record for each unique combination of dedup_cols based on order_by_col (descending)
        """
        window_spec = Window.partitionBy(*dedup_cols).orderBy(F.col(order_by_col).desc())
        df = (
            df.withColumn("row_num", F.row_number().over(window_spec))
              .filter(F.col("row_num") == 1)
              .drop("row_num")
        )
        return df

    def create_or_upsert(self, spark, df, key_cols, table, cdc):
        """
        Perform upsert (merge) with schema evolution.
        Keeps only latest record based on CDC column.
        """
        # spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

        table_full = f"`e-commerce-project`.{table}"

        # ✅ Deduplicate before merge
        df = self.dedup(df, key_cols, cdc)

        if not spark.catalog.tableExists(table_full):
            (
                df.write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(table_full)
            )
            print(f"✅ Created new Delta table: {table_full}")
        else:
            merge_cond = " AND ".join([f"src.{c}=trg.{c}" for c in key_cols])
            delta_table = DeltaTable.forName(spark, table_full)

            (
                delta_table.alias("trg")
                .merge(df.alias("src"), merge_cond)
                .whenMatchedUpdateAll(condition=f"src.{cdc} >= trg.{cdc}")
                .whenNotMatchedInsertAll()
                .execute()
            )

            print(f"✅ Upsert completed for table: {table_full}")
    @staticmethod
    def atlas_convert_dates(obj):
        """Recursively convert datetime/date objects to ISO 8601 strings."""
        if isinstance(obj, dict):
            return {k: transformations.atlas_convert_dates(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [transformations.atlas_convert_dates(i) for i in obj]
        elif isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        else:
            return obj  
    @staticmethod
    def upsert_to_atlas(spark_df, collection_name, unique_key,mongo_uri,db,client):
        """
        Upserts records into MongoDB Atlas using bulk write.
        """
        # ✅ FIXED: use F.col instead of string condition
        df_latest = spark_df.filter(F.col("__END_AT").isNull())

        # ✅ FIXED: use correct recursive function
        records = [transformations.atlas_convert_dates(r)
                   for r in df_latest.toPandas().to_dict("records")]

        from pymongo import UpdateOne
        operations = [
            UpdateOne({unique_key: r[unique_key]}, {"$set": r}, upsert=True)
            for r in records
        ]

        if not operations:
            print(f"⚠️ No active records to upsert for {collection_name}")
            return

        collection = db[collection_name]
        result = collection.bulk_write(operations, ordered=False)

        print(f"✅ Upserted into '{collection_name}' — "
              f"Matched: {result.matched_count}, "
              f"Modified: {result.modified_count}, "
              f"Upserted: {len(result.upserted_ids)}")

