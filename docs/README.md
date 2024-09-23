# Improving Oracle APEX API Integration Performance with DuckDB

In the world of web applications, every integration point can significantly impact performance. Oracle APEX is no exception to this rule. When dealing with numerous APIs and large data loads, especially in monolithic architectures, developers often find themselves constrained. The traditional approach involves:

1. Multiple calls by the APEX engine
2. Storing data in temporary tables or collections
3. Relying on the database engine for operations
4. Sending results back to APEX for browser rendering

Many APEX deployments follow this monolithic pattern, which can limit flexibility and performance. But what if there was a way to dramatically enhance both without a major architectural overhaul? That's exactly what we're here to explore.

Enter **DuckDB**: a game-changer in the world of database systems. DuckDB is a lightning-fast, in-process database that doesn't even require installation. It's the perfect complement to monolithic APEX deployments, offering significant performance boosts without the need for complex OSI-level restructuring.


### Use Case: Real-time Flight Delay Analysis Dashboard

Imagine an airport operations team tasked with monitoring and responding to flight delays across multiple major airports. They need a system that can:

<ol>
        <li>Continuously fetch and store real-time flight delay data</li>
        <li>Provide instant analytics on delay patterns</li>
        <li>Allow for quick, ad-hoc queries across large datasets</li>
        <li>Present the information in an intuitive, web-based dashboard</li>
    </ol>

By integrating Oracle APEX with DuckDB, we created a powerful solution that meets these needs:

  <ul>
        <li><strong>Data Collection:</strong> Using APScheduler, the system automatically fetches flight delay data from the Airlabs API every 30 minutes, storing it efficiently in DuckDB.</li>
        <li><strong>Rapid Analytics:</strong> DuckDB's in-memory, columnar storage allows for lightning-fast SQL queries on the flight delay data, even as it grows to millions of records.</li>
        <li><strong>Flexible Querying:</strong> The operations team can use Oracle APEX to build custom queries, filtering delays by airport, time range, or delay duration.</li>
    </ul>

For example:

```sql 
SELECT AVG(delayed_time) as avg_delay, COUNT(*) as total_flights
FROM flight_delays
WHERE airport_code = 'JFK' 
  AND dep_time BETWEEN '2023-09-21 06:00:00' AND '2023-09-21 12:00:00'
```


### Real-time Dashboard with Unified API

Oracle APEX provides a user-friendly interface where team members can view summary statistics, trends, and detailed flight information. The dashboard's data is sourced through a single, unified FastAPI endpoint, which acts as a gateway to DuckDB's rapid data processing capabilities.


<p align="center">
<img src="../docs/duckDB-oracleAPEX.svg" alt="Architectural Diagram of the Flight Data System" width="500" height="500">
</p>
<p align="center">
Figure 1: Architectural Diagram of the Use Case
</p>

### Key Components

1. **Oracle APEX Frontend**
   - Offers an intuitive, customizable dashboard interface
   - Uses AJAX calls to fetch data from the FastAPI endpoint

2. **FastAPI Backend**
   - Serves as a single entry point for all data requests
   - Handles authentication and request validation
   - Translates frontend requests into appropriate DuckDB queries

3. **DuckDB Integration**
   - Performs high-speed data processing and analysis
   - Executes complex queries efficiently on the flight dataset

4. **Data Flow**
   - APEX dashboard sends requests to the FastAPI endpoint
   - FastAPI processes the request and queries DuckDB
   - DuckDB executes the query and returns results to FastAPI
   - FastAPI formats the response and sends it back to APEX

### Benefits of This Architecture

1. **Simplified API Management**: A single API endpoint reduces complexity in frontend development and maintenance.

2. **Enhanced Security**: Centralized authentication and authorization in the FastAPI layer.

3. **Flexible Data Processing**: FastAPI can preprocess or aggregate data before sending it to the frontend, reducing load on the client side.

4. **Scalability**: As the dataset grows, DuckDB's efficient storage and query optimization ensure that performance remains snappy. The FastAPI layer can be easily scaled horizontally if needed.

5. **Real-time Updates**: The architecture supports real-time or near-real-time updates to the dashboard by implementing WebSocket connections in FastAPI.

6. **Reduced Network Overhead**: By consolidating requests through a single API, we minimize the number of network calls, potentially improving overall system performance.

### Implementation Considerations



This integration allows the airport operations team to make data-driven decisions quickly, improving their response to delays and enhancing overall airport efficiency.

The application fetches flight delay data (departures and arrivals) from Airlabs and stores it in a local DuckDB database. This data can be queried via the `/summary` and `/delays` endpoints. We also set up a scheduler using APScheduler to periodically pull new delay information.





The combination of DuckDB and Oracle APEX creates a powerful synergy that can revolutionize how you handle data in your applications. Throughout this blog post, we'll venture into the numerous benefits of this integration:

- How can this pairing supercharge your app's performance?
- What cost savings might you realize?
- Why will developers find this solution easier to work with?

From turbocharged query speeds to streamlined data management, the advantages are substantial. You'll discover how this dynamic duo can effortlessly handle big data, delivering the snappy response times that users love. We'll also explore real-world examples and provide practical tips to help you get started.

Whether you're an experienced developer or new to the world of data-driven applications, this post has invaluable insights for you. Get ready to unlock the full potential of your web applications with DuckDB and Oracle APEX!

### Main Components of the Code:


1. **FastAPI**: A modern, fast (high-performance) web framework for building APIs with Python 3.7+ based on standard Python type hints.
2. **DuckDB**: An in-process SQL OLAP database management system used for efficient analytics and storage.
3. **Airlabs.co API**: The source of real-time flight delay data.
4. **APScheduler**: A Python library for scheduling tasks like pulling data periodically.

### How the API Works

- **/fetch_delays**: Fetches the latest flight delays from Airlabs based on filters (arrivals or departures) and stores them in DuckDB, preventing duplicate entries using a unique key (combination of flight number, departure time, and flight type).
- **/summary**: Provides a summary of average delays and total flights for a specific airport within a given time range.
- **/delays**: Returns all flight delays stored in DuckDB.




### DuckDB: In-Process Analytics Database

[DuckDB](https://duckdb.org/) is an open-source in-process SQL OLAP database management system. It’s designed to support fast, analytical queries and works seamlessly within applications without needing a server. We use DuckDB here to store flight delay information retrieved from the Airlabs API.

Key features of DuckDB in this project:
- **In-memory database**: DuckDB operates in memory, making it fast for analytics.
- **Persistent storage**: We create and store a table `flight_delays.db` on disk for persistent data storage.
- **SQL interface**: We use SQL queries to insert, update, and retrieve data efficiently.

**Efficient Data Storage with DuckDB**:
   - **In-Memory Analytics**: DuckDB is an in-process SQL OLAP database management system designed for analytical workloads. It allows you to execute complex SQL queries directly within your Python process without the overhead of setting up a separate database server.
   - **Columnar Storage**: DuckDB uses a columnar storage format optimized for read-heavy operations, which is ideal for analytics on large datasets like flight delay information.
   - **Integration with DataFrames**: DuckDB can seamlessly interact with Pandas DataFrames or Apache Arrow tables, allowing you to load data into DuckDB and query it using SQL while still working within the Python ecosystem.

**Advanced Data Analysis**:
   - **SQL Queries**: Leverage DuckDB's powerful SQL capabilities to perform complex queries, aggregations, and joins that might be cumbersome or inefficient in plain Python or Pandas.
   - **Performance**: DuckDB is optimized for analytical queries and can handle large volumes of data efficiently, making your analysis faster compared to using in-memory DataFrames alone.
   - **Concurrent Processing**: DuckDB supports parallel query execution, which can speed up data processing tasks on multicore processors.



### FastAPI: Building the API

FastAPI is a modern Python framework that allows us to quickly build APIs with automatic validation and documentation. In this project, we use FastAPI to:
- Define endpoints (`/fetch_delays`, `/summary`, and `/delays`).
- Validate input data using Pydantic models.
- Serve responses in JSON format.

FastAPI automatically generates interactive API docs using OpenAPI standards, which can be accessed by visiting `/docs` once the server is running.



### APScheduler: Automating API Calls

We use APScheduler to schedule tasks that periodically fetch new flight delay data from the Airlabs API. This allows our application to keep its data up to date automatically. The fetch operation runs every 30 minutes for both departures and arrivals.

Key features:
- **Interval-based scheduling**: Fetches flight data every 30 minutes.
- **Conflict resolution**: Ensures duplicate flight records (same flight, time, and type) are not inserted twice into the database.



### Airlabs.co API: Flight Data Source

[Airlabs.co](https://airlabs.co) provides real-time flight delay information. You need to register for an API key to access their data. Airlabs' data can be used for personal projects but **cannot be resold**. You must abide by their [usage policy](https://airlabs.co/legal).

### How to Use Airlabs API:
1. **Register**: Sign up for an Airlabs API key at [Airlabs.co](https://airlabs.co).
2. **Store API Key**: Once you have your key, store it in a `.env` file like so:
   ```bash
   AIRLABS_API_KEY=your_airlabs_api_key_here
   ```



### Installation: Required Python Packages

You’ll need the following Python packages to run this project:

- **FastAPI**: The API framework. Install it with:
  ```bash
  pip install fastapi
  ```

- **Uvicorn**: ASGI server for FastAPI:
  ```bash
  pip install uvicorn
  ```

- **APScheduler**: Used for scheduling background tasks:
  ```bash
  pip install apscheduler
  ```

- **DuckDB**: Lightweight in-process database for storing delay data:
  ```bash
  pip install duckdb
  ```

- **Python-dotenv**: For loading environment variables from a `.env` file:
  ```bash
  pip install python-dotenv
  ```

- **Requests**: To make API calls to Airlabs:
  ```bash
  pip install requests
  ```

- **Pydantic**: For data validation and parsing with FastAPI:
  ```bash
  pip install pydantic
  ```

- **All at once**: Install using the requirements file
  ```bash
  pip install -r requirements.txt    
  ```

### Running the Application

To start the API, run the following command:

```bash
uvicorn main:app --reload
```

This will start the FastAPI application, and you can access it at `http://127.0.0.1:8000`.



### Curl Examples to Interact with the API

You can use the following curl commands to interact with the API and test its functionality.

### Set Today's Date
First, export today's date as a variable for easy reuse in curl commands:

```bash
export TODAY=$(date +"%Y-%m-%d")
```

#### Example 1: Fetch Delay Data for Departures and Arrivals
Fetch delay data for departures and arrivals with a minimum delay of 30 minutes:

```bash
# Fetch departures delays with a minimum delay of 30 minutes
curl "http://127.0.0.1:8000/fetch_delays?flight_type=departures&min_delayed_time=30"

# Fetch arrivals delays with a minimum delay of 30 minutes
curl "http://127.0.0.1:8000/fetch_delays?flight_type=arrivals&min_delayed_time=30"
```

#### Example 2: Get a Summary of Delays for JFK for Today
Retrieve a summary of delays at JFK for today's date:

```bash
curl "http://127.0.0.1:8000/summary?airport_code=JFK&date_time_from=${TODAY}%2000:00:00&date_time_to=${TODAY}%2023:59:59"
```

#### Example 3: Get a Summary of Delays for LAX Between 6:00 AM and 12:00 PM
Retrieve a summary of delays at LAX for the morning period of today's date:

```bash
curl "http://127.0.0.1:8000/summary?airport_code=LAX&date_time_from=${TODAY}%2006:00:00&date_time_to=${TODAY}%2012:00:00"
```

#### Example 4: Get a Summary of Delays for ORD Between 12:00 PM and 6:00 PM
Retrieve a summary of delays at ORD (Chicago O'Hare) for the afternoon period:

```bash
curl "http://127.0.0.1:8000/summary?airport_code=ORD&date_time_from=${TODAY}%2012:00:00&date_time_to=${TODAY}%2018:00:00"
```

#### Example 5: Get a Summary of Delays for ATL Without Time Filtering
Retrieve a summary of all stored flight delays for ATL (Atlanta) without applying a specific time filter:

```bash
curl -s "http://127.0.0.1:8000/summary?airport_code=ATL" | jq
```

#### Example 6: Get a Summary for ATL With a Time Range
Retrieve a summary of delays for ATL using today’s date as a range filter:

```bash
curl -s "http://127.0.0.1:8000/summary?airport_code=ATL&date_time_from=${TODAY}%2000:00:00&date_time_to=${TODAY}%2023:59:59" | jq
```

#### Example 7: Get All Stored Delays
Retrieve all stored flight delays from DuckDB:

```bash
curl -s "http://127.0.0.1:8000/delays" | jq
```

## Accessing the Code

To get started with the Flight Data System, visit our GitLab repository at:

[https://gitlab.com/your-organization/flight-data-system](https://gitlab.com/your-organization/flight-data-system)

From there, you can clone the repository or download the code directly. Refer to the `README.md` file in the repository for specific setup and running instructions.

<a href="mailto:info@geahsoft.com" style="display: inline-block; padding: 10px 20px; background-color: #007bff; color: white; text-decoration: none; border-radius: 9999px; font-weight: bold; text-align: center; transition: background-color 0.3s ease;">Find more about our services - Contact Us</a>