# run_product.py
import os
from pyspark.sql import SparkSession
from src.data_interaction import generate_kpis_and_visualizations 
from src.config import REPORTS_DIR 


print("Iniciando la sesión de Spark")
spark = SparkSession.builder \
    .appName("DataProductRunner") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()
print("Sesión de Spark creada.")

try:
    print("Ejecutando la generación de KPIs y visualizaciones")
    generate_kpis_and_visualizations(spark)
    print("Generación de KPIs y visualizaciones completada")

except Exception as e:
    print(f"Ocurrió un error al ejecutar el script de data_product: {e}")
finally:
    print("Deteniendo la sesión de Spark")
    spark.stop()
    print("Sesión de Spark detenida")