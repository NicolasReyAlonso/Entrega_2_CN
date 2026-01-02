# steam_aggregation_by_year.py
# Agregación de datos de juegos de Steam por año de lanzamiento
import sys
import logging
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col, count, avg, sum as spark_sum, first
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
    
    # Agregación por año de lanzamiento: estadísticas de juegos
    yearly_df = df.groupBy("release_year") \
        .agg(
            count("*").alias("total_games"),
            avg("price").alias("avg_price"),
            spark_sum("recommendations").alias("total_recommendations"),
            avg("recommendations").alias("avg_recommendations")
        ) \
        .orderBy("release_year")
    
    output_dynamic_frame = DynamicFrame.fromDF(yearly_df, glueContext, "output")
    
    logger.info(f"Registros agregados: {output_dynamic_frame.count()}")
    
    # Escribir usando GlueContext
    glueContext.write_dynamic_frame.from_options(
        frame=output_dynamic_frame,
        connection_type="s3",
        connection_options={
            "path": output_path,
            "partitionKeys": ["release_year"]
        },
        format="parquet",
        format_options={"compression": "snappy"}
    )
    
    logger.info(f"Completado. Registros: {yearly_df.count()}")

if __name__ == "__main__":
    main()