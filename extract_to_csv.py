"""
Prueba técnica para Data Intelligence

1.2 Extracción de Información

José Agustín Moreno Larios, 2025-08-17
"""

# Importa librerías
import csv
import psycopg
from psycopg import sql

# Definimos algunas funciones para extracción
def guarda_csv(csv_path, query_method, **kwargs):
    """
    Ejecuta la función definida con query_method
    y guarda los resultados en csv_path.
    """

    # Llama a query_method con sus respectivos argumentos
    resultados = query_method(**kwargs)
    
    # Abre archivo csv y escribe
    with open(csv_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # Obten nombres de columnas para el encabezado
        header = [desc[0] for desc in kwargs['cursor'].description]
        # Escribe línea de encabezado
        writer.writerow(header)
        # Escribe los resultados a csv
        for resultado in resultados:
            writer.writerow(resultado)


def filtra_por_company(cursor, company_name):
    """
    Obtiene la tabla a partir del nombre de la compañía
    """
    # Define solicitud al servidor
    query = sql.SQL(
        """
        SELECT * FROM prueba
        WHERE company_name = %s
        """
        )
    # Ejecuta
    cursor.execute(query, [company_name])
    # Regresa los resultados.
    return cursor.fetchall()

def filtra_company_status(cursor, company_name, status):
    """
    Filtra la tabla con el nombre de la compañía y el estado de pago
    """
    query = sql.SQL(
        """
        SELECT * FROM prueba
        WHERE company_name = %s
        AND status = %s
        """
        )
    # Ejecuta
    cursor.execute(query, [company_name, status])
    # Regresa los resultados.
    return cursor.fetchall()

def consigue_todo(cursor):
    """
    Obtiene toda la información de la tabla.
    """
    query = sql.SQL(
        """
        SELECT * FROM prueba
        """
        )
    # Ejecuta
    cursor.execute(query)
    # Regresa los resultados.
    return cursor.fetchall()

# Conecta con psycopg para bases de datos PostgreSQL
# Uso PostgreSQL ya que se utilizará en otro ejercicio más adelante
# Asumo que existe la base de datos en el servidor
conn = psycopg.connect(dbname="prueba",
                       user="postgres",
                       password="secure_password",
                       host="postgres_db",
                       port="5432")

# Abre cursor para realizar operaciones
cur = conn.cursor()

# Filtra por compañía
filtra_por_company(cur, "Muebles chidos")
# conn.commit()

guarda_csv('operaciones_muebles.csv', filtra_por_company,
           company_name="Muebles chidos",
           cursor=cur
           )

guarda_csv('estado_de_operaciones.csv', filtra_company_status,
           company_name='MiPasajefy',
           cursor=cur,
           status='voided'
           )

guarda_csv('todas_operaciones.csv',
           consigue_todo,
           cursor=cur)


# Cierra conección
conn.close()
