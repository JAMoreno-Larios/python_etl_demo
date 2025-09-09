# syntax=docker/dockerfile:1
FROM python:latest
WORKDIR /code
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
EXPOSE 5432:5432
COPY . .
CMD ["bash"]

