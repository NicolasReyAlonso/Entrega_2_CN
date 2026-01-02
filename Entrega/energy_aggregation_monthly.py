# steam_aggregation_by_genre.py
# Agregación de datos de juegos de Steam por género
import sys
import logging
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col, count, avg, sum as spark_sum, split, explode
from awsglue.dynamicframe import DynamicFrame

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    args = getResolvedOptions(sys.argv, ['database', 'table', 'output_path'])
    database = args['database']
    table = args['table']
    output_path = args['output_path']

    logger.info(f"Database: {database}, Table: {table}, Output: {output_path}")
    
    sc = SparkContext()
    glueContext = GlueContext(sc)

    # Leer desde Glue Catalog usando GlueContext
    dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
        database=database,
        table_name=table
    )
    
    # Convertir a Spark DataFrame
    df = dynamic_frame.toDF()
    df.printSchema()
    logger.info(f"Registros leídos: {df.count()}")
    
    # Explotar géneros (cada juego puede tener múltiples géneros separados por ;)
    df_exploded = df.withColumn("genre", explode(split(col("genres"), ";")))
    
    # Agregación por género: estadísticas de juegos
    genre_df = df_exploded.groupBy("genre") \
        .agg(
            count("*").alias("total_games"),
            avg("price").alias("avg_price"),
            spark_sum("recommendations").alias("total_recommendations"),
            avg("recommendations").alias("avg_recommendations")
        ) \
        .orderBy(col("total_games").desc())
    
    output_dynamic_frame = DynamicFrame.fromDF(genre_df, glueContext, "output")
    
    logger.info(f"Registros agregados: {output_dynamic_frame.count()}")
    
    # Escribir usando GlueContext
    glueContext.write_dynamic_frame.from_options(
        frame=output_dynamic_frame,
        connection_type="s3",
        connection_options={
            "path": output_path,
            "partitionKeys": ["genre"]
        },
        format="parquet",
        format_options={"compression": "snappy"}
    )
    
    logger.info(f"Completado. Registros: {genre_df.count()}")

if __name__ == "__main__":
    main()