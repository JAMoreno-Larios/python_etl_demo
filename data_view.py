"""
Prueba técnica para Data Intelligence

1.5 Vista en SQL

José Agustín Moreno Larios, 2025-08-19
"""

# Importa librerías
import psycopg
from psycopg import sql

# Definimos funciones

def create_view(cursor):
    """
    Vamos a crear una vista de las transacciones.
    """
    query_transacciones = sql.SQL(
        """
        SELECT 
            charges.company_id, status, 
            updated_at, SUM(amount)
        FROM transacciones.charges
        GROUP BY updated_at, company_id, status;
        """
    )
    query_vista = sql.SQL(
        """
        CREATE VIEW transacciones.diario AS
            {}
        """).format(query_transacciones)
    
    # Ejecutamos
    cursor.execute(query_vista)


# Conecta con psycopg para bases de datos PostgreSQL
# Asumo que existe la base de datos en el servidor
conn = psycopg.connect(dbname="prueba",
                       user="postgres",
                       password="secure_password",
                       host="postgres_db",
                       port="5432")

# Abre cursor para realizar operaciones
cur = conn.cursor()

# Crea vista
create_view(cur)

# Termina la transacción
conn.commit()

# Cierra conección
conn.close()

# Fin
print("Terminamos la creación de una vista")
