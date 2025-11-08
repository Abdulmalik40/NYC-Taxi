🚖 NYC Taxi Data Pipeline: Landing → Bronze → Silver → Gold
Built on Databricks with Apache Spark & Delta Lake
A modern data pipeline ingesting API-sourced NYC Yellow Taxi trip data, transforming it through Medallion Architecture for analytics and reporting.

📌 Overview
This pipeline processes raw NYC Yellow Taxi trip data obtained via an API into a structured, analytical-ready format. It follows the Medallion Architecture (Landing → Bronze → Silver → Gold) to ensure data quality, traceability, and usability.

The pipeline enriches raw trips with geographic context using a taxi_zone_lookup table and aggregates daily metrics for business intelligence dashboards.

🗺️ Data Flow Architecture
SVG content

📁 1. Landing Zone
Catalog: nyctaxi
Schema: landing
Storage: /Volumes/nyctaxi/landing/data_sources/

📄 Raw Data Files
File Pattern: Yellow*tripdata*{year-month}.parquet
Relative Path: ./nyctaxi_yellow/{year-month}
⚙️ Schema (yellow_trips_raw)
VendorID
Taxi vendor identifier
tpep_pickup_datetime
Pickup timestamp
tpep_dropoff_datetime
Dropoff timestamp
passenger_count
Number of passengers
trip_distance
Distance traveled in miles
RatecodeID
Rate code type
store_and_fwd_flag
Store-and-forward flag
PULocationID
Pickup location ID
DOLocationID
Dropoff location ID
payment_type
Payment method
fare_amount
Base fare amount
extra
Extra charges
mta_tax
MTA tax
tip_amount
Tip amount
tolls_amount
Tolls paid
improvement_surcharge
Improvement surcharge
total_amount
Total trip cost
congestion_surcharge
Congestion surcharge
airport_fee
Airport fee
cbd_congestion_fee
Central Business District congestion fee
processed_timestamp
Timestamp when data was ingested

✅ Purpose: Immutable storage of raw, unprocessed data as received from the API.

🧱 2. Bronze Layer
Catalog: nyctaxi
Schema: bronze

📄 Table: yellow_trips_raw
Source: Directly loaded from Landing Zone.
Structure: Identical to raw file schema.
Delta Lake: Enables ACID transactions, versioning, and schema enforcement.
✅ Purpose: First persistent layer — preserves original structure while enabling basic validation and metadata tracking.

🌟 3. Silver Layer
Catalog: nyctaxi
Schema: silver

📄 Table: yellow_trips_cleaned
Source: bronze.yellow_trips_raw
Transformations Applied:
Cast and validate datatypes.
Handle nulls and outliers.
Add processed_timestamp for lineage tracking.
Derive trip_duration_minutes from pickup/dropoff times.
🔍 Key Added Fields:
trip_duration_minutes
Calculated duration of trip in minutes
pu_location_id
Cleaned pickup location ID
do_location_id
Cleaned dropoff location ID

✅ Purpose: Cleaned, validated, and standardized data ready for enrichment.

📄 Table: taxi_zone_lookup
Source: Static lookup table stored in ./lookup/

LocationID
Unique zone identifier
Borough
NYC borough name
Zone
Neighborhood or zone name
service_zone
Service area type
effective_date
Date this record became active
end_date
Date this record ended (if applicable)

🔄 Note: This table is joined with yellow_trips_cleaned to add geographic context.

📄 Table: yellow_trips_enriched
Sources:
silver.yellow_trips_cleaned
silver.taxi_zone_lookup (joined twice: for pickup and dropoff)
Transformations:
Join with taxi_zone_lookup to add:
pu_borough, do_borough
pu_zone, do_zone
Validate and clean enriched fields.
Preserve all cleaned fields from yellow_trips_cleaned.
✅ Purpose: Enriched dataset with geographic context for spatial analysis and reporting.

🏆 4. Gold Layer
Catalog: nyctaxi
Schema: gold

📄 Table: daily_trip_summary
Source: silver.yellow_trips_enriched
Aggregation Logic:
Group by tpep_pickup_datetime truncated to date (pickup_date)
Compute daily KPIs
📊 Summary Metrics:
pickup_date
Date of trip (YYYY-MM-DD)
total_trips
Total number of trips
avg_passengers_per_trip
Average passengers per trip
avg_distance_per_trip
Average distance per trip (miles)
avg_fare_per_trip
Average fare per trip
max_fare
Maximum fare recorded
total_revenue
Sum of
total_amount
across all trips

✅ Purpose: Business-ready aggregated view for dashboards, reports, and executive insights.

🔄 Pipeline Execution Flow
Ingest: API data lands as Parquet files in Landing.
Load to Bronze: Ingest raw files into bronze.yellow_trips_raw.
Clean to Silver: Apply transformations → silver.yellow_trips_cleaned.
Enrich with Lookup: Join with taxi_zone_lookup → silver.yellow_trips_enriched.
Aggregate to Gold: Daily summaries → gold.daily_trip_summary.
🛠️ Technology Stack
Platform: Databricks
Compute Engine: Apache Spark
Storage Format: Delta Lake (ACID-compliant, versioned tables)
Data Source: External API (raw Parquet files)
Architecture: Medallion (Landing → Bronze → Silver → Gold)
📈 Use Cases
Operational Dashboards: Monitor daily trip volume, revenue, and passenger trends.
Geospatial Analysis: Understand trip patterns by borough/zone.
Anomaly Detection: Identify outliers in fare, distance, or duration.
Machine Learning: Feature engineering for demand forecasting or pricing models.
