import os
import pandas as pd
from datetime import datetime


BASE_DIR = 'data_lake'
LANDING_ZONE = os.path.join(BASE_DIR, 'landing')
RAW_ZONE = os.path.join(BASE_DIR, 'raw')
CURATED_ZONE = os.path.join(BASE_DIR, 'curated')

LANDING_IMDB_DIR = os.path.join(LANDING_ZONE, 'imdb')
RAW_IMDB_DIR = os.path.join(RAW_ZONE, 'imdb')

IMDB_TITLE_BASICS_FILE_LANDING = os.path.join(LANDING_IMDB_DIR, 'title.basics.tsv') 
IMDB_TITLE_RATINGS_FILE_LANDING = os.path.join(LANDING_IMDB_DIR, 'title.ratings.tsv') 
IMDB_NAME_BASICS_FILE_LANDING = os.path.join(LANDING_IMDB_DIR, 'name.basics.tsv') 
IMDB_TITLE_AKAS_FILE_LANDING = os.path.join(LANDING_IMDB_DIR, 'title.akas.tsv') 
IMDB_TITLE_CREW_FILE_LANDING = os.path.join(LANDING_IMDB_DIR, 'title.crew.tsv')
IMDB_TITLE_PRINCIPALS_FILE_LANDING = os.path.join(LANDING_IMDB_DIR, 'title.principals.tsv') 

IMDB_BASICS_FILE_RAW = os.path.join(RAW_ZONE, 'imdb_title_basics.parquet')
IMDB_RATINGS_FILE_RAW = os.path.join(RAW_ZONE, 'imdb_ratings.parquet')
IMDB_NAME_BASICS_FILE_CURATED = os.path.join(RAW_ZONE, 'imdb_name_basics.parquet')

for zone_path in [LANDING_ZONE, RAW_ZONE, CURATED_ZONE]:
    os.makedirs(zone_path, exist_ok=True)
    print(f"Directorio creado/verificado: {zone_path}")


def clean_tsv_file(input_file, output_file, chunk_size=100000, max_rows=100000):
    print(f"Cleaning file: {input_file}")
    #Aca proponemoos procesar los archivos en chunks q basicamente permite rendir mejor al cargar grandes volumenes de datos
    cleaned_chunks = []
    chunk_count = 0
    total_collected = 0
    for chunk in pd.read_csv(input_file, sep='\t', chunksize=chunk_size, low_memory=False):
        if total_collected >= max_rows:
            break
        chunk_count += 1
        #Acá como se puede ver se limpia un poco lo que viene siendo las filas vacias o repetidas
        chunk = chunk.dropna(how='all')
        chunk = chunk.drop_duplicates()
        #Despues en esta seccion lo que se hace es se borran los espacios en blanco si son strings
        for col in chunk.select_dtypes(include=['object']).columns:
            chunk[col] = chunk[col].astype(str).str.strip()
        #Parseamos strings 0 y 1 a booleanos y tmb los strings mismos a booleanos
        for col in chunk.columns:
            if col in ['isAdult', 'isOriginalTitle']:
                chunk[col] = chunk[col].map({'0': False, '1': True, 'false': False, 'true': True, '': False}).fillna(False)
            elif col in ['startYear', 'endYear', 'runtimeMinutes', 'birthYear', 'deathYear', 'ordering', 'averageRating', 'numVotes']:
                chunk[col] = pd.to_numeric(chunk[col], errors='coerce')
        #Se limitan las rows por temas de eficiencia
        rows_to_add = min(max_rows - total_collected, len(chunk))
        if rows_to_add > 0:
            cleaned_chunks.append(chunk.iloc[:rows_to_add])
            total_collected += rows_to_add
    #Mezclamos los chunks
    df = pd.concat(cleaned_chunks, ignore_index=True)
    #Sacamos los duplicados
    df = df.drop_duplicates()
    #Se guarda en parquet
    df.to_parquet(output_file, index=False)


def clean_all_tsv_files():
    print("Arrancamos a limpiar :)")
    #Se crea la carpeta de raw con la fecha y hora actual
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    raw_run_dir = os.path.join(RAW_ZONE, timestamp)
    os.makedirs(raw_run_dir, exist_ok=True)
    file_mappings = [
        (IMDB_TITLE_BASICS_FILE_LANDING, os.path.join(raw_run_dir, 'imdb_title_basics.parquet')),
        (IMDB_TITLE_RATINGS_FILE_LANDING, os.path.join(raw_run_dir, 'imdb_ratings.parquet')),
        (IMDB_NAME_BASICS_FILE_LANDING, os.path.join(raw_run_dir, 'imdb_name_basics.parquet')),
        (IMDB_TITLE_AKAS_FILE_LANDING, os.path.join(raw_run_dir, 'imdb_title_akas.parquet')),
        (IMDB_TITLE_CREW_FILE_LANDING, os.path.join(raw_run_dir, 'imdb_title_crew.parquet')),
        (IMDB_TITLE_PRINCIPALS_FILE_LANDING, os.path.join(raw_run_dir, 'imdb_title_principals.parquet'))
    ]
    
    #El for que corre por los archivos para limpiarlos
    for input_file, output_file in file_mappings:
        if os.path.exists(input_file):
            clean_tsv_file(input_file, output_file)
        else:
            print(f"File not found: {input_file}")
        print("-" * 50)
    
    print("Limpieza concretada")


if __name__ == "__main__":
    #Si no existen las carpetas se crean
    for zone_path in [LANDING_ZONE, RAW_ZONE, CURATED_ZONE]:
        os.makedirs(zone_path, exist_ok=True)
        print(f"Directorio creado/verificado: {zone_path}")

    clean_all_tsv_files()

