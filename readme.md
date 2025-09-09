# python_etl_demo

Python implementation of an ETL pipeline using Python, PostgreSQL and Docker.
To run the pipeline, execute `runner.sh`

`runner.sh` will call five scripts in Python:
  - csv_loader.py: Performs initial data ingestion
  - extract_to_csv.py: Gets information from the Postgres database and returns it to a `.csv` file
  - data_transformation.py: Adapts database information to conform to a new specified schema. It carries out the necessary transformations for its proper functioning.
  - data_propagation.py: Divides the information housed in the new schema into two related tables.
  - data_view.py: Create a view for PostgreSQL using the information from daily transactions.

Implementación en Python de un pipeline de datos usando Python, PostgreSQL y Docker.

Para correr el pipeline, ejecute
`runner.sh`

`runner.sh` llamará a cinco scripts en Python:

- csv_loader.py: realiza la ingesta inicial de datos
- extract_to_csv.py: obtiene información de la base de datos Postgres y la devuelve a un archivo `.csv`
- data_transformation.py: adapta la información de base de datos para conformarse con un nuevo esquema especificado. Realiza las transformaciones necesarias para su buen funcionamiento.
- data_propagation.py: reparte la información alojada en el nuevo esquema en dos tablas relacionadas.
- data_view.py: Crea una vista para PostgreSQL usando la información de las transacciones diarias.
