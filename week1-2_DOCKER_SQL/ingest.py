import pandas as pd
from sqlalchemy import create_engine
import time
import os


def load_csv_batches(path, batch_size=100000):
    """Yield CSV chunks as DataFrames."""
    return pd.read_csv(path, chunksize=batch_size)


def connect_sql_alchemy():
    """Attempt to connect to Postgres up to 10 times."""
    for i in range(10):
        try:
            engine = create_engine("postgresql://root:root@pgdatabase:5432/ny_taxi")
            engine.connect()
            print("Connected to Postgres!")
            return engine
        except Exception as e:
            print(f"Waiting for Postgres to be ready... Try {i+1}/10")
            time.sleep(3)
    print("Failed to connect to Postgres after 10 attempts.")
    return None


def load_to_postgres(batches_iter, engine):
    """Load CSV batches into Postgres."""
    t_start = time.time()
    count = 0


    # Process the first batch to create the table
    first_batch_df = next(batches_iter)


    print("Creating table header...")
   
    first_batch_df.head(0).to_sql(
        name='ny_taxi_data_loaded',
        con=engine,
        if_exists='replace',
        index=False
    )
    print("Table header created successfully.")


    # Insert the first batch
    count += 1
    print(f"Inserting batch {count}...")
    b_start = time.time()
    first_batch_df.to_sql(
        name='ny_taxi_data_loaded',
        con=engine,
        if_exists='append',
        index=False,
        chunksize=10000,
        method='multi'
    )
    print(f"Inserted batch {count}. Time taken: {time.time() - b_start:.3f} seconds.\n")


    # Insert remaining batches
    for batch_df in batches_iter:
        count += 1
        print(f"Inserting batch {count}...")
        b_start = time.time()
        batch_df.to_sql(
            name='ny_taxi_data_loaded',
            con=engine,
            if_exists='append',
            index=False,
            chunksize=10000,
            method='multi'
        )
        print(f"Inserted batch {count}. Time taken: {time.time() - b_start:.3f} seconds.\n")


    print(f"Completed! Total time: {time.time() - t_start:.3f} seconds over {count} batches.")


if __name__ == "__main__":
    try:
        print("Starting CSV ingestion...\n")
        csv_file = "yellow_tripdata_2021-01.csv.gz"


        batches = load_csv_batches(csv_file)


        engine = connect_sql_alchemy()
        if engine:
            load_to_postgres(batches_iter=batches, engine=engine)
            print("CSV ingestion completed successfully!")
        else:
            print("Could not connect to Postgres. Exiting...")


    except Exception as e:
        print(f"Error during CSV ingestion: {e}")


