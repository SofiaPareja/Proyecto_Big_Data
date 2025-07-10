# src/data_raw.py

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim,lit,  expr
from pyspark.sql.types import  BooleanType, StringType
from src.config import LANDING_ZONE_PATH, RAW_ZONE_PATH, SCHEMAS

def load_and_clean_to_raw_zone(spark: SparkSession):
    
    os.makedirs(RAW_ZONE_PATH, exist_ok=True)
    print("iniciando la carga y limpieza de datos a Raw Zone")

    for key, schema in SCHEMAS.items():
        #asumiendo que los archivos TSV ya están en la landing zone
        tsv_path = os.path.join(LANDING_ZONE_PATH, f"{key}.tsv")
        parquet_output_path = os.path.join(RAW_ZONE_PATH, key.replace('.', '_'))

        if not os.path.exists(tsv_path):
            print(f"no se encontro el archivo en Landing Zone: {tsv_path}")
            continue

        
        try:
            df = spark.read.csv(tsv_path, sep="\t", header=True, nullValue="\\N", schema=schema)
            df = df.dropna(how='all')
            df = df.dropDuplicates()

            #se hace casteo de tipos y trim de strings
            for column_name in df.columns:
                if isinstance(df.schema[column_name].dataType, StringType):
                    df = df.withColumn(column_name, trim(col(column_name)))
                if column_name in ['isAdult', 'isOriginalTitle']:
                    df = df.withColumn(column_name,
                                       when(col(column_name) == "1", True)
                                       .when(col(column_name) == "0", False)
                                       .otherwise(False).cast(BooleanType()))
                elif column_name in ['startYear', 'endYear', 'runtimeMinutes', 'ordering', 'birthYear', 'deathYear', 'numVotes']:
                    #convertir a IntegerType, manejando nulos y posibles errores de conversion
                    df = df.withColumn(column_name, expr(f"try_cast({column_name} AS INT)"))
                elif column_name in ['averageRating']:
                    #pasarlo a FloatType
                    df = df.withColumn(column_name, expr(f"try_cast({column_name} AS FLOAT)"))
                
            df.write.parquet(parquet_output_path, mode="overwrite")
            print(f"{key} procesado y guardado en Raw Zone.")
        except Exception as e:
            print(f"Error al cargar {key} a Raw Zone: {e}")
           
    print("Carga y limpieza a Raw Zone completada.")