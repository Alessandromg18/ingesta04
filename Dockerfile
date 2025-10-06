# Usa una imagen base de Python 3.10 slim
FROM python:3.10-slim

# Instalación de dependencias del sistema
RUN apt-get update && \
    apt-get install -y libpq-dev gcc && \
    pip install --upgrade pip && \
    pip install cryptography psycopg2 pandas boto3 sqlalchemy

# Copiar el código y el archivo de configuración al contenedor
COPY . /app

# Establecer el directorio de trabajo dentro del contenedor
WORKDIR /app

# Establecer las variables de entorno desde un archivo .env (si lo tienes)
COPY .env /app/.env

# Comando para ejecutar el script
CMD ["python", "export_to_s3.py"]
