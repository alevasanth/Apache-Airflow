from  airflow.sdk import dag, task
from pendulum import datetime
from airflow.timetables.interval import CronDataIntervalTimetable
import sqlite3
import os

@dag(
    schedule=CronDataIntervalTimetable("@daily", timezone="Canada/Eastern"),
    start_date=datetime(year=2026, month=2, day=15, tz="Canada/Eastern"),
    end_date=datetime(year=2026, month=2, day=28, tz="Canada/Eastern"),
    catchup=True
)
def incremental_sql_dag():

    @task.python
    def incremental_extraction(**kwargs):
        date_interval_start = kwargs['data_interval_start']
        date_interval_end = kwargs['data_interval_end']
        print(f"Extracting data for the interval: {date_interval_start} to {date_interval_end}")
        
        # SQLite database file path
        db_path = "/tmp/airflow_data.db"
        
        # Create connection
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Create table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                name TEXT,
                email TEXT,
                created_at TIMESTAMP
            )
        """)
        
        # Insert sample data if table is empty
        cursor.execute("SELECT COUNT(*) FROM users")
        if cursor.fetchone()[0] == 0:
            sample_data = [
                (1, 'Alice', 'alice@example.com', '2026-02-15 12:00:00'),
                (2, 'Bob', 'bob@example.com', '2026-02-16 14:30:00'),
                (3, 'Charlie', 'charlie@example.com', '2026-02-17 09:15:00'),
                (4, 'David', 'david@example.com', '2026-02-18 16:45:00'),
                (5, 'Eve', 'eve@example.com', '2026-02-19 11:20:00'),
                (6, 'Frank', 'frank@example.com', '2026-02-20 13:00:00'),
                (7, 'Grace', 'grace@example.com', '2026-02-21 10:30:00'),
                (8, 'Henry', 'henry@example.com', '2026-02-22 15:45:00'),
                (9, 'Ivy', 'ivy@example.com', '2026-02-23 14:15:00'),
                (10, 'Jack', 'jack@example.com', '2026-02-23 17:30:00'),
                (11, 'Karen', 'karen@example.com', '2026-02-23 10:45:00'),
                (12, 'Leo', 'leo@example.com', '2026-02-24 13:15:00'),
            ]
            cursor.executemany(
                "INSERT INTO users (id, name, email, created_at) VALUES (?, ?, ?, ?)",
                sample_data
            )
            conn.commit()
            print("Sample data inserted")
        
        # SQL query to extract incremental data
        sql_query = f"""
            SELECT id, name, email, created_at 
            FROM users 
            WHERE created_at >= datetime('{date_interval_start}')
            AND created_at < datetime('{date_interval_end}')
            ORDER BY created_at
        """
        
        print(f"Executing SQL: {sql_query}")
        
        # Execute query and fetch results
        cursor.execute(sql_query)
        extracted_data = cursor.fetchall()
        
        print(f"Extracted {len(extracted_data)} records")
        for record in extracted_data:
            print(f"  - ID: {record[0]}, Name: {record[1]}, Email: {record[2]}, Created: {record[3]}")
        
        conn.close()
        return extracted_data
    
    @task.python
    def incremental_data_processing(**kwargs):
        ti = kwargs['ti']
        
        # Pull extracted data from previous task
        extracted_data = ti.xcom_pull(task_ids='incremental_extraction')
        
        print(f"\n{'='*70}")
        print(f"PROCESSING {len(extracted_data)} EXTRACTED RECORD(S)")
        print(f"{'='*70}")
        
        if not extracted_data:
            print("No data to process")
            return "No records to process"
        
        # Process each record
        processed_records = []
        for record in extracted_data:
            record_id, name, email, created_at = record
            processed_record = {
                "id": record_id,
                "name": name.upper(),  # Example: convert name to uppercase
                "email": email,
                "created_at": created_at,
                "processed": True
            }
            processed_records.append(processed_record)
            print(f"  ✓ Processed: {name} ({email}) - Created: {created_at}")
        
        print(f"{'='*70}")
        print(f"Total records processed: {len(processed_records)}")
        print(f"{'='*70}\n")
        
        # Push processed records to XCom for downstream tasks
        ti.xcom_push(key='processed_records', value=processed_records)
        
        return processed_records
    
    # Set task dependencies
    incremental_extraction() >> incremental_data_processing()

# Generate/Initiate the DAG
incremental_sql_dag()