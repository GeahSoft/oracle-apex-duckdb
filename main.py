from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field, conint
import duckdb
import requests
import os
from dotenv import load_dotenv
from typing import List, Optional, Annotated
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import logging

# Load environment variables from .env file
load_dotenv()
AIRLABS_API_KEY = os.getenv('AIRLABS_API_KEY')

if not AIRLABS_API_KEY:
    raise Exception("Airlabs API key not found. Please set AIRLABS_API_KEY in your environment.")

# Initialize FastAPI app
app = FastAPI(title="Flight Delay API")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define the data model using Pydantic
class DelayRecord(BaseModel):
    airline_iata: str
    flight_iata: str
    dep_iata: Annotated[str, Field(min_length=3, max_length=3, pattern="^[A-Z]{3}$")]
    dep_icao: Optional[str]
    arr_iata: Annotated[str, Field(min_length=3, max_length=3, pattern="^[A-Z]{3}$")]
    arr_icao: Optional[str]
    delayed: conint(ge=0)
    flight_type: str  # 'arrivals' or 'departures'
    dep_time: datetime
    arr_time: datetime

class DelaySummary(BaseModel):
    airport_code: str
    arrival_departure: str
    average_delay: float
    total_flights: int

# Connect to DuckDB (persistent storage)
conn = duckdb.connect('flight_delays.db')
cursor = conn.cursor()

# Create the delays table with primary key to prevent duplicates
cursor.execute('''
CREATE TABLE IF NOT EXISTS delays (
    airline_iata VARCHAR,
    flight_iata VARCHAR,
    dep_iata VARCHAR,
    dep_icao VARCHAR,
    arr_iata VARCHAR,
    arr_icao VARCHAR,
    delayed INTEGER,
    flight_type VARCHAR,  -- 'arrivals' or 'departures'
    dep_time TIMESTAMP,
    arr_time TIMESTAMP,
    PRIMARY KEY (flight_iata, dep_time, flight_type)
)
''')

def fetch_delays_task(
    flight_type: str,
    min_delayed_time: int,
    arrival_airport_code: Optional[str] = None,
    departure_airport_code: Optional[str] = None
):
    """
    Scheduled task to fetch delay information from the Airlabs API with specified filters.
    """
    try:
        # Define the API endpoint and parameters
        url = 'https://airlabs.co/api/v9/delays'
        params = {
            'api_key': AIRLABS_API_KEY,
            'delay': min_delayed_time,
            'type': flight_type
        }

        # Add airport code filters if provided
        if arrival_airport_code:
            params['arr_iata'] = arrival_airport_code.upper()
        if departure_airport_code:
            params['dep_iata'] = departure_airport_code.upper()

        # Make the API request
        response = requests.get(url, params=params)
        data = response.json()

        if data.get('error'):
            logger.error(f"Error fetching data: {data['error']['message']}")
            return

        # Extract the delays data
        delays = data.get('response', [])
        if not delays:
            logger.info("No delay data available with the specified filters.")
            return

        # Prepare data for insertion
        records = []
        for item in delays:
            # Parse times
            dep_time_utc = item.get('dep_time_utc')
            arr_time_utc = item.get('arr_time_utc')

            # Skip if times are missing
            if not dep_time_utc or not arr_time_utc:
                continue

            # Convert times to datetime objects
            try:
                dep_time_dt = datetime.strptime(dep_time_utc, '%Y-%m-%d %H:%M')
                arr_time_dt = datetime.strptime(arr_time_utc, '%Y-%m-%d %H:%M')
            except ValueError:
                logger.warning(f"Invalid date format for flight {item.get('flight_iata')}")
                continue

            # Get the 'delayed' value
            delayed = item.get('delayed', 0)
            if not isinstance(delayed, int):
                try:
                    delayed = int(delayed)
                except (ValueError, TypeError):
                    delayed = 0

            # Only process if delayed >= min_delayed_time
            if delayed >= min_delayed_time:
                dep_iata = item.get('dep_iata', '')
                dep_icao = item.get('dep_icao', '')
                arr_iata = item.get('arr_iata', '')
                arr_icao = item.get('arr_icao', '')
                records.append((
                    item.get('airline_iata', ''),
                    item.get('flight_iata', ''),
                    dep_iata,
                    dep_icao,
                    arr_iata,
                    arr_icao,
                    delayed,
                    flight_type,
                    dep_time_dt,
                    arr_time_dt
                ))

        if not records:
            logger.info("No delay data available after processing.")
            return

        # Insert data into the database with conflict handling to prevent duplicates
        cursor.executemany('''
            INSERT INTO delays (
                airline_iata, flight_iata, dep_iata, dep_icao,
                arr_iata, arr_icao, delayed, flight_type, dep_time, arr_time
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (flight_iata, dep_time, flight_type) DO UPDATE SET
                airline_iata=excluded.airline_iata,
                dep_iata=excluded.dep_iata,
                dep_icao=excluded.dep_icao,
                arr_iata=excluded.arr_iata,
                arr_icao=excluded.arr_icao,
                delayed=excluded.delayed,
                arr_time=excluded.arr_time
        ''', records)
        conn.commit()

        logger.info(f"{len(records)} records inserted/updated successfully.")

    except Exception as e:
        logger.error("An error occurred during fetch_delays_task", exc_info=True)

# Start the scheduler
scheduler = BackgroundScheduler()

# Schedule the fetch_delays_task with default parameters
scheduler.add_job(
    fetch_delays_task,
    'interval',
    minutes=30,  # Adjust the interval as needed
    kwargs={
        'flight_type': 'departures',  # or 'arrivals'
        'min_delayed_time': 30
        # 'departure_airport_code': 'JFK',  # Optional
        # 'arrival_airport_code': 'LAX'     # Optional
    }
)

scheduler.add_job(
    fetch_delays_task,
    'interval',
    minutes=30,  # Adjust the interval as needed
    kwargs={
        'flight_type': 'arrivals',  # or 'arrivals'
        'min_delayed_time': 30
        # 'departure_airport_code': 'JFK',  # Optional
        # 'arrival_airport_code': 'LAX'     # Optional
    }
)

scheduler.start()

# Ensure the scheduler is shut down when the app exits
@app.on_event("shutdown")
def shutdown_event():
    scheduler.shutdown()

@app.get('/fetch_delays', status_code=200)
def fetch_delays(
    flight_type: str = Query(..., pattern="^(arrivals|departures)$"),
    min_delayed_time: int = Query(..., ge=0),
    arrival_airport_code: Optional[str] = Query(
        None, min_length=3, max_length=3, pattern="^[A-Z]{3}$"
    ),
    departure_airport_code: Optional[str] = Query(
        None, min_length=3, max_length=3, pattern="^[A-Z]{3}$"
    )
):
    """
    Fetch delay information from the Airlabs API with specified filters and store it in the database.
    """
    try:
        fetch_delays_task(
            flight_type=flight_type,
            min_delayed_time=min_delayed_time,
            arrival_airport_code=arrival_airport_code,
            departure_airport_code=departure_airport_code
        )
        return {'status': 'success', 'message': 'Data fetched and stored successfully.'}
    except Exception as e:
        logger.error("An error occurred in /fetch_delays endpoint", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

from datetime import datetime, timezone

@app.get('/summary', response_model=List[DelaySummary])
def get_summary(
    airport_code: Annotated[str, Query(min_length=3, max_length=3, pattern="^[A-Z]{3}$")],
    date_time_from: Optional[str] = Query(None),
    date_time_to: Optional[str] = Query(None)
):
    """
    Get the summary of delays for a specific airport within a time range.
    """
    try:
        airport_code = airport_code.upper()
        summary = []

        # Parse date_time_from and date_time_to as UTC datetime objects
        if date_time_from:
            date_from = datetime.strptime(date_time_from, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
        else:
            date_from = None
        if date_time_to:
            date_to = datetime.strptime(date_time_to, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
        else:
            date_to = None

        # Departures
        dep_params = [airport_code]
        dep_where_conditions = ['dep_iata = ?']
        if date_from and date_to:
            dep_where_conditions.append('dep_time BETWEEN ? AND ?')
            dep_params.extend([date_from, date_to])
        elif date_from:
            dep_where_conditions.append('dep_time >= ?')
            dep_params.append(date_from)
        elif date_to:
            dep_where_conditions.append('dep_time <= ?')
            dep_params.append(date_to)

        dep_where_clause = 'WHERE ' + ' AND '.join(dep_where_conditions)

        dep_query = f'''
            SELECT 'departures' AS arrival_departure, AVG(delayed) AS average_delay, COUNT(*) AS total_flights
            FROM delays
            {dep_where_clause}
        '''
        dep_result = cursor.execute(dep_query, dep_params).fetchone()

        # Arrivals
        arr_params = [airport_code]
        arr_where_conditions = ['arr_iata = ?']
        if date_from and date_to:
            arr_where_conditions.append('arr_time BETWEEN ? AND ?')
            arr_params.extend([date_from, date_to])
        elif date_from:
            arr_where_conditions.append('arr_time >= ?')
            arr_params.append(date_from)
        elif date_to:
            arr_where_conditions.append('arr_time <= ?')
            arr_params.append(date_to)

        arr_where_clause = 'WHERE ' + ' AND '.join(arr_where_conditions)

        arr_query = f'''
            SELECT 'arrivals' AS arrival_departure, AVG(delayed) AS average_delay, COUNT(*) AS total_flights
            FROM delays
            {arr_where_clause}
        '''
        arr_result = cursor.execute(arr_query, arr_params).fetchone()

        # Compile results
        if dep_result and dep_result[2] > 0:
            summary.append({
                'airport_code': airport_code,
                'arrival_departure': dep_result[0],
                'average_delay': float(dep_result[1]),
                'total_flights': dep_result[2]
            })
        if arr_result and arr_result[2] > 0:
            summary.append({
                'airport_code': airport_code,
                'arrival_departure': arr_result[0],
                'average_delay': float(arr_result[1]),
                'total_flights': arr_result[2]
            })

        if not summary:
            raise HTTPException(status_code=404, detail="No data found for the specified parameters.")

        return summary

    except HTTPException as e:
        raise e
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use 'YYYY-MM-DD HH:MM:SS'.")
    except Exception as e:
        logger.error("An error occurred in /summary endpoint", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get('/delays')
def get_all_delays():
    """
    Get all delay records.
    """
    try:
        result = cursor.execute('''
            SELECT airline_iata, flight_iata, dep_iata, dep_icao,
                   arr_iata, arr_icao, delayed, flight_type, dep_time, arr_time
            FROM delays
        ''').fetchall()

        # Convert to list of dictionaries
        delays = [
            {
                'airline_iata': row[0],
                'flight_iata': row[1],
                'dep_iata': row[2],
                'dep_icao': row[3],
                'arr_iata': row[4],
                'arr_icao': row[5],
                'delayed': row[6],
                'flight_type': row[7],
                'dep_time': row[8].isoformat(),
                'arr_time': row[9].isoformat()
            }
            for row in result
        ]
        return delays

    except Exception as e:
        logger.error("An error occurred in /delays endpoint", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
