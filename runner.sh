#!/bin/bash

# Script para ejecutar cada paso de la instalación y tarea

# Instalamos la imagen de Docker
docker compose up --build -d

# Esperamos
sleep 5

# Usamos nuestra aplicación para ejecutar cada script
docker exec -it tty_for_python python csv_loader.py
docker exec -it tty_for_python python extract_to_csv.py
docker exec -it tty_for_python python data_transformation.py
docker exec -it tty_for_python python data_propagation.py
docker exec -it tty_for_python python data_view.py

