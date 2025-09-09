"""
Prueba técnica para Data Intelligence

1.1 Carga de Información

José Agustín Moreno Larios, 2025-08-17
"""

# Importa librerías
import csv
import psycopg
from psycopg import sql

# Conecta con psycopg para bases de datos PostgreSQL
# Asumo que existe la base de datos en el servidor
conn = psycopg.connect(dbname="prueba",
                       user="postgres",
                       password="secure_password",
                       host="postgres_db",
                       port="5432")

# Abre cursor para realizar operaciones
cur = conn.cursor()

# Crea tabla manualmente
cur.execute(
    """
            CREATE TABLE prueba (
                id varchar NOT NULL,
                company_name varchar,
                company_id varchar NOT NULL,
                amount numeric NOT NULL,
                status varchar NOT NULL,
                created_at timestamp NOT NULL,
                updated_at timestamp
                )
            """
)

# Abre archivo csv, remueve líneas en blanco, escribe a base de datos
with open('data_prueba_tecnica.csv', newline='') as input_file:
    # Crea objeto lector
    reader = csv.reader(input_file)
    # Salta línea de encabezado
    next(reader)
    # Vamos a verificar que cada línea sea válida antes de insertar
    for row in reader:
        # Verificamos si tenemos una línea válida
        if row:
            # Vamos a sanitizar cada línea
            str_id = row[0]
            str_company_name = row[1]
            str_company_id = row[2]
            decimal_amount = float(row[3]) # Si hay un valor en extremo grande,
                                           # es convertido a inf
            str_status = row[4]
            str_date_created_at = row[5]
            str_date_updated_at = row[6]
            # Escribe cada línea a la base de datos
            cur.execute(
                """
                INSERT INTO prueba (id, company_name,
                company_id, amount, status, created_at, updated_at)
                VALUES (%s, %s, %s, 
                %s::numeric, %s, 
                %s::timestamp, NULLIF(%s, '')::timestamp
                )
                """, [str_id, str_company_name, str_company_id,
                      decimal_amount, str_status, str_date_created_at,
                      str_date_updated_at]
            )

# Termina la transacción
conn.commit()

# Cierra conección
conn.close()

# Fin
print("Terminamos la extracción de datos")
