# Use official Python slim image
FROM python:3.9-slim


# Install system dependencies
RUN apt-get update && apt-get install -y \
    wget \
    && rm -rf /var/lib/apt/lists/*


# Install Python dependencies
RUN pip install --no-cache-dir pandas sqlalchemy psycopg2-binary


# Set working directory
WORKDIR /app


# Copy Python script into the container
# Copy Python script and CSV into the container
#COPY ingest.py yellow_tripdata_2021-01.csv.gz ./
COPY ingest_zone.py taxi_zone_lookup.csv ./



# Default command to run the script
ENTRYPOINT ["python", "ingest_zone.py"]
