
import os
from pyspark.sql import SparkSession
from src.data_curated import transform_to_curated_zone
from src.data_interaction import  generate_kpis_and_visualizations 
from src.config import RAW_ZONE_PATH, CURATED_ZONE_PATH, REGION_OF_INTEREST 

if __name__ == "__main__":
    print("Iniciando la ejecución del pipeline completo: Curación de Datos + Producto de Datos...")

    spark = SparkSession.builder \
        .appName("FullDataPipeline") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN") 

    try:
        print("\n--- Ejecutando la fase de Curación de Datos ---")
        transform_to_curated_zone(spark)
        print("Fase de Curación de Datos completada exitosamente.")

        print("\n--- Ejecutando la fase de Generación de Producto de Datos ---")
        generate_kpis_and_visualizations(spark) 
        print("Fase de Generación de Producto de Datos completada exitosamente.")

        print("\nPipeline completo ejecutado con éxito.")

    except Exception as e:
        print(f"\n¡Error fatal durante la ejecución del pipeline!: {e}")
    finally:
        spark.stop()
        print("SparkSession detenida.")