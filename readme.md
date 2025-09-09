# Prueba 1

Implementación en Python de un pipeline
de datos usando PostgreSQL y Docker.

Para correr el pipeline, ejecute
`runner.sh`

`runner.sh` llamará a cinco scripts en Python:

- csv_loader.py: realiza la ingesta inicial de datos
- extract_to_csv.py: obtiene información de la base de datos Postgres y la devuelve a un archivo `.csv`
- data_transformation.py: adapta la información de base de datos para conformarse con un nuevo esquema especificado. Realiza las transformaciones necesarias para su buen funcionamiento.
- data_propagation.py: reparte la información alojada en el nuevo esquema en dos tablas relacionadas.
- data_view.py: Crea una vista para PostgreSQL usando la información de las transacciones diarias.
