"""
Prueba técnica para Data Intelligence

1.4 Dispersión de la información
A partir del esquema implementado anteriormente, crear dos
tablas: una llamada 'charges' con información de las transacciones y
otra llamada ' companies' donde se incluye la información de las
compañías.

José Agustín Moreno Larios, 2025-08-18
"""

# Importa librerías
import psycopg
from psycopg import sql

# Definimos funciones


def create_companies(cursor):
    """
    Define la solicitud para extraer los datos sobre las compañías.
    Sabemos que hay valores duplicados, usaremos la clave DISTINCT
    """
    query = sql.SQL(
            """
            CREATE TABLE transacciones.companies AS
                SELECT 
                    DISTINCT company_id, company_name
                FROM transacciones.datos_base
            """
    )
    # Ejecutamos
    cursor.execute(query)
    # Hacemos que company_id sea clave principal
    query = sql.SQL(
            """
            ALTER TABLE transacciones.companies
                ADD PRIMARY KEY (company_id)
            """)
    # Ejecutamos
    cursor.execute(query)


def create_charges(cursor):
    """
    Define la solicitud para extraer los datos de las transacciones
    Usaremos CREATE TABLE ... AS para crear la tabla directamente
    a partir de nuestro query. Una vez hecho esto, definimos
    a id y echa como clave primaria de la tabla, company_id como
    clave extranjera, relacionando la tabla con companies.
    """
    query = sql.SQL(
            """
            CREATE TABLE transacciones.charges AS
                SELECT 
                    id, company_id, amount, 
                    status, created_at, updated_at
                FROM transacciones.datos_base
            """
    )
    # Ejecutamos
    cursor.execute(query)
    # Fijamos claves primarias
    query = sql.SQL(
            """
            ALTER TABLE transacciones.charges
                ADD PRIMARY KEY (id)
            """)
    # Ejecutamos
    cursor.execute(query)
    # Segunda parte
    query = sql.SQL(
        """
        ALTER TABLE transacciones.charges
            ADD FOREIGN KEY (company_id) REFERENCES
            transacciones.companies (company_id)
        """
    )
    # Ejecutamos
    cursor.execute(query)


# Conecta con psycopg para bases de datos PostgreSQL
# Asumo que existe la base de datos en el servidor
conn = psycopg.connect(dbname="prueba",
                       user="postgres",
                       password="secure_password",
                       host="postgres_db",
                       port="5432")

# Abre cursor para realizar operaciones
cur = conn.cursor()

# Crea tablas
create_companies(cur)
create_charges(cur)

# Termina la transacción
conn.commit()

# Cierra conección
conn.close()

# Fin
print("Terminamos la dispersión de datos")
