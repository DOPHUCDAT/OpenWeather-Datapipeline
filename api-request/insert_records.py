from request_api import main_request
from dotenv import load_dotenv
import psycopg2
import os

load_dotenv()
def connect_db(db_name, db_user, db_password):
    """Connect to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            dbname=f"{db_name}",
            user=f"{db_user}",
            password=f"{db_password}"
        )
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to the database: {e}")
        raise

def create_table(conn):
    """Create the weather_data table if it doesn't exist."""
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS weather
            CREATE TABLE IF NOT EXISTS weather.weather_data (
                id SERIAL PRIMARY KEY,
                city TEXT,
                temperature FLOAT,
                weather_descriptions TEXT,
                wind_speed FLOAT,
                time TIMESTAMP,
                inserted_at TIMESTAMP DEFAULT NOW(),
                utc_offset TEXT
            )
        """)
        conn.commit()
        cursor.close()
    except psycopg2.Error as e:
        print(f"Error creating table: {e}")
        raise

def insert_record(conn, data):
    """Insert record into the weather_data table."""
    try:
        cursror = conn.cursor()
        cursror.execute("""
            INSERT INTO weather.weather_data(
                city,
                temperature,
                weather_descriptions,
                wind_speed,
                time,
                inserted_at,
                utc_offset
            ) VALUES (%s, %s, %s, %s, %s, NOW(), %s)
        """, (
            data['location']['name'],
            data['current']['temperature'],
            data['current']['weather_descriptions'],
            data['current']['wind_speed'],
            data['location']['time'],
            data['location']['utc_offset']
        ))
        conn.commit()
        cursror.close()
    except psycopg2.Error as e:
        print(f"Error inserting record: {e}")
        raise


def main():
    try:
        db_name = os.getenv("DB_NAME")
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")
        data = main_request()
        conn = connect_db(db_name, db_user, db_password)
        create_table(conn)
        insert_record(conn, data)
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()