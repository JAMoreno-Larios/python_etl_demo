"""
Prueba técnica para Data Intelligence

1.3 Transformación
Implementar un esquema y transformar la información obtenida de la
base de datos.

José Agustín Moreno Larios, 2025-08-18
"""

# Importa librerías
import decimal
import secrets
import base64
import psycopg
from psycopg import sql

# Definimos funciones auxiliares

def fix_companies(company_id, company_name):
    """
    Al inspeccionar la base de datos original podemos
    encontrar que tenemos dos compañías.
    Esta función se encargará de que exista una
    company_id por nombre
    """
    valid_company_ids = {
            'MiPasajefy': 'cbf1c8b09cd5b549416d49d220a40cbd317f952e',
            'Muebles chidos': '8f642dc67fccf861548dfe1c761ce22f795e91f0'
    }
    # Invierte el diccionario
    valid_company_names = {v: k for k, v in valid_company_ids.items()}
    
    # Checa si la id de compañía funciona
    if company_id not in valid_company_ids.values():
        if company_name in valid_company_ids.keys():
            # Si existe el nombre, usa la clave correspondiente
            new_id = valid_company_ids[company_name]
            new_name = company_name
        else:
            raise ValueError('Invalid company_id, company_name pair')
    else:
        new_id = company_id
        if company_name not in valid_company_names.values():
            new_name = valid_company_names[company_id]
        else:
            new_name = company_name
    # Regresa nuevos nombres
    return (new_id, new_name)


def hex_to_base85_trunc(hex_str: str,
                        max_len: int = None) -> str:
    """
    Convierte una cadena que representa una clave
    en hexadecimal y la transforma a una cadena
    en base85. 
    Podemos truncar la clave de ser necesario
    Nota, las ID originales son de 40 digitos en hex, aparentemente.
    Si usamos ASCII85, la cadena sería de 25 dígitos, más cerca de lo pedido
    Podemos truncar a 24 caracteres, aunque podría haber colisiones
    """
    # Convierte a bytes
    byte_array = bytearray.fromhex(hex_str)
    # Codifica a b85
    b85 = base64.b85encode(byte_array)
    # Convierte a str
    codificado = b85.decode('utf-8')
    # Trunca cadena
    truncado = codificado[:max_len]
    # Fin
    return truncado


def format_amount(amount: decimal.Decimal,
                  precision: int = 16,
                  scale: int = 2):
    """
    Convierte un valor decimal.Decimal(...)
    PostgreSQL al formato especificado en nuestro esquema,
    es decir, que tenga una precisión de 16 dígitos, usando
    2 decimales.
    Modificaremos el contexto de la clase decimal para tener
    el comportamiento deseado.
    """
    # Obtiene el contexto para el uso de decimales
    ctx = decimal.getcontext()
    # Fija precisión
    ctx.prec = precision
    ctx.clamp=1
    ctx.Emin=-2
    ctx.Emax=13
    ctx.rounding= decimal.ROUND_FLOOR  # Cualquier valor superior 
                                       # al limite se redondea
    ctx.traps[decimal.Overflow] = False  # No disparamos la excepción
    # Consigue los números después del punto decimal
    q = decimal.Decimal(10) ** -scale      # Escala de dos -> '0.01'
    # Crea nuevo decimal a partir de amount y cuantiza para tener sólo
    # los decimales deseados, si hay sobreflujo, fijar a infinito.
    try:
        new_amount = ctx.create_decimal(amount).quantize(q)
    except decimal.InvalidOperation as e:
        # Se activa si se usa un valor infinito. Lo reemplazamos
        # por el valor más grande que podamos representar
        new_amount = ctx.create_decimal(10e14).quantize(q)
    # Fin
    return new_amount


# Conecta con psycopg para bases de datos PostgreSQL
# Asumo que existe la base de datos en el servidor
conn = psycopg.connect(dbname="prueba",
                       user="postgres",
                       password="secure_password",
                       host="postgres_db",
                       port="5432")

# Abre cursor para realizar operaciones
cur = conn.cursor()

# Crea esquema y tabla para los datos base
# Usemos la id, company_id y created_at como identificadores
# únicos en nuestra tabla.
cur.execute("""
            CREATE SCHEMA transacciones
                CREATE TABLE datos_base (
                    id varchar(24) NOT NULL,
                    company_name varchar(130),
                    company_id varchar(24) NOT NULL,
                    amount decimal(16, 2) NOT NULL,
                    status varchar(30) NOT NULL,
                    created_at timestamp NOT NULL,
                    updated_at timestamp,
                    UNIQUE (id, company_id, created_at)
                    )
            """)
# Nota, las ID originales son de 40 digitos en hex, aparentemente.
# Si usamos ASCII85, la cadena sería de 25 dígitos, más cerca de lo pedido
# Podemos truncar a 24 caracteres, aunque podría haber colisiones

# Vamos a tomar los datos de la tabla de prueba existente
query = sql.SQL(
        """
        SELECT * FROM prueba 
        """
        )
# Ejecuta
cur.execute(query)

# Iteremos paso por paso
for datos in cur.fetchall():
    # Nombraremos cada elemento que recuperemos para
    # legibilidad
    str_id = datos[0]
    str_company_name = datos[1]
    str_company_id = datos[2]
    decimal_amount = datos[3]
    str_status = datos[4]
    date_created_at = datos[5]
    date_updated_at = datos[6]

    # Revisamos si las ID son válidas, corrige si no lo son.
    # Después convertiremos las ID a base85 y truncamos el último caracter

    # Checa si el id no está vacío. Si lo está, genera uno provisional
    if str_id == '':
        str_id = secrets.token_hex(20)  # Genera una cadena hex de 40 caracteres

    # Arregla la id y nombre de compañía
    valid_company_id, valid_company_name = fix_companies(str_company_id,
                                                         str_company_name)
    if (len(str_id) > 24):
        new_id = hex_to_base85_trunc(str_id, 24)
    else:
        new_id = str_id
    if (len(valid_company_id) > 24):
        new_company_id = hex_to_base85_trunc(valid_company_id, 24)
    else:
        new_company_id = valid_company_id
    # Convertiremos el valor amount al nuevo formato
    new_amount = format_amount(decimal_amount)

    # Escribe cada línea a la base de datos
    cur.execute(
        """
        INSERT INTO transacciones.datos_base
        (id, company_name,
        company_id, amount, status, created_at, updated_at)
        VALUES (%s, %s, %s, 
        %s, %s, 
        %s, %s
        )
        """, [new_id, valid_company_name, new_company_id,
              new_amount, str_status, date_created_at,
              date_updated_at]
    )

# Termina la transacción
conn.commit()

# Cierra conección
conn.close()

# Fin
print("Terminamos la transformación de datos")
