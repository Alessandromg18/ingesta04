import os
import pandas as pd
import boto3
import json
from dotenv import load_dotenv
from sqlalchemy import create_engine
from datetime import datetime

# Cargar variables del archivo .env
load_dotenv()

# --- Configuración ---
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")

S3_BUCKET = os.getenv("S3_BUCKET")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# Verificar que las variables de entorno necesarias estén presentes
required_vars = [DB_HOST, DB_USER, DB_PASS, DB_NAME, S3_BUCKET]
if any(var is None for var in required_vars):
    raise ValueError("Faltan variables de entorno necesarias.")

# Diccionario de tablas y sus correspondientes directorios en S3
TABLES = {
    "dashboard_data": "dashboard_data/",
    "dashboard_published_data": "dashboard_published_data/"
}

# Esquemas de las tablas (definidos con los tipos de datos de cada columna)
SCHEMAS = {
    "dashboard_data": [
        { "Name": "id", "Type": "bigint" },
        { "Name": "admin_id", "Type": "bigint" },
        { "Name": "date_posted", "Type": "date" },
        { "Name": "engagement", "Type": "double" },
        { "Name": "likes", "Type": "int" },
        { "Name": "post_id", "Type": "string" },
        { "Name": "posturl", "Type": "string" },
        { "Name": "used_hash_tag", "Type": "string" },
        { "Name": "username_tiktok_account", "Type": "string" },
        { "Name": "views", "Type": "int" },
        { "Name": "publication_id", "Type": "bigint" }
    ],
    "dashboard_published_data": [
        { "Name": "id", "Type": "bigint" }
    ]
}

# --- Funciones ---
def obtener_datos_tabla(schema, table):
    """Obtiene todos los datos de la tabla especificada."""
    engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    df = pd.read_sql(f'SELECT * FROM "{schema}"."{table}";', engine)
    return df

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Limpia strings y asegura compatibilidad con Athena (eliminando caracteres no válidos)."""
    for col in df.select_dtypes(include=["object", "bool"]).columns:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(r"[\r\n\t]", " ", regex=True)
            .str.replace(r"[^\x20-\x7Eáéíóúñ]", "", regex=True)  # Permitir caracteres UTF-8 comunes
        )
    return df

def cast_types(df: pd.DataFrame, schema: list) -> pd.DataFrame:
    """Convierte columnas a los tipos definidos en el esquema."""
    for col in schema:
        name, typ = col["Name"], col["Type"]
        if name not in df.columns:
            continue
        if typ == "bigint":
            df[name] = pd.to_numeric(df[name], errors="coerce", downcast="integer")
        elif typ == "double":
            df[name] = pd.to_numeric(df[name], errors="coerce", downcast="float")
        elif typ == "int":
            df[name] = pd.to_numeric(df[name], errors="coerce", downcast="integer")
        elif typ == "date":
            df[name] = pd.to_datetime(df[name], errors="coerce").dt.strftime("%Y-%m-%d")
        elif typ == "timestamp":
            df[name] = pd.to_datetime(df[name], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
        else:  # string
            df[name] = df[name].astype(str)
    return df

def export_to_ndjson(df: pd.DataFrame, filename: str):
    """Exporta el DataFrame a NDJSON (una fila = un JSON en una sola línea)."""
    with open(filename, "w", encoding="utf-8") as f:
        for _, row in df.iterrows():
            f.write(json.dumps(row.to_dict(), ensure_ascii=False) + "\n")

def subir_a_s3_json(filename, table_name):
    """Sube el archivo NDJSON a S3."""
    s3 = boto3.client("s3", region_name=AWS_REGION)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    s3_key = f"{TABLES[table_name]}{table_name}_{timestamp}.json"
    
    try:
        s3.upload_file(filename, S3_BUCKET, s3_key)
        print(f"✅ Subido a S3: s3://{S3_BUCKET}/{s3_key}")
    except Exception as e:
        print(f"⚠️ Error al subir a S3: {e}")

# --- Ejecución principal ---
def limpiar_bucket():
    """Limpia los archivos JSON del bucket S3."""
    print("🔄 Limpiando bucket...")
    try:
        s3 = boto3.client("s3", region_name=AWS_REGION)
        objects = s3.list_objects_v2(Bucket=S3_BUCKET)
        if "Contents" in objects:
            for obj in objects["Contents"]:
                if obj["Key"].endswith(".json"):
                    s3.delete_object(Bucket=S3_BUCKET, Key=obj["Key"])
            print("✅ Archivos previos eliminados.")
        else:
            print("ℹ️ No hay archivos previos en el bucket.")
    except Exception as e:
        print(f"⚠️ Error al limpiar bucket: {e}")

def main():
    # 1. Limpiar bucket
    limpiar_bucket()

    # 2. Exportar tablas
    for table, folder in TABLES.items():
        try:
            print(f"📥 Exportando tabla: {table}")
            # Obtener datos de la tabla
            df = obtener_datos_tabla("public", table)  # Asumiendo esquema 'public'
            
            if not df.empty:
                # Limpiar y convertir tipos
                df = clean_dataframe(df)
                df = cast_types(df, SCHEMAS.get(table, []))  # Usar el esquema correspondiente

                # Exportar a NDJSON y subir a S3
                filename = f"{table}.json"
                export_to_ndjson(df, filename)
                subir_a_s3_json(filename, table)

                # Eliminar archivo temporal después de subir
                os.remove(filename)
            else:
                print(f"⚠️ La tabla {table} está vacía, se omite.")
        except Exception as e:
            print(f"⚠️ Error con la tabla {table}: {e}")

if __name__ == "__main__":
    main()
