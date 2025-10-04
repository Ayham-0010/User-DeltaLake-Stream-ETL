### User-DeltaLake-Stream-ETL ###

This project implements a real-time Change Data Capture (CDC) pipeline using:

Apache Kafka as the event streaming backbone<br>
Apache Spark Structured Streaming for data ingestion, transformation, and processing Delta Lake for reliable storage, schema enforcement, and ACID transactions.<br>
The pipeline streams CRUD operations from a PostgreSQL database (captured via Debezium → Kafka) and writes them into Delta Lake tables with merge logic to maintain up-to-date datasets.<br>

### Key Features ###

Real-Time Ingestion: Streams PostgreSQL CDC events from Kafka into Delta Lake.<br>
Schema-Driven Parsing: Structured parsing of Debezium CDC payloads using PySpark schemas.<br>
Delta Lake MERGE Logic: Ensures inserts, updates, and deletes are reflected correctly.<br>
Scalable & Fault-Tolerant: Uses Spark checkpointing for exactly-once guarantees.<br>
Modular Design: Configurable paths, schemas, and Kafka topics for multiple entities.<br>

### Data Flow ###

Debezium → Kafka<br>
PostgreSQL CDC events are published to Kafka topics.<br>

Kafka → Spark Structured Streaming<br>
Spark consumes raw CDC JSON messages from Kafka.<br>
Messages are parsed into structured DataFrames with schemas.<br>

Spark → Delta Lake<br>
Each entity (e.g., account, organization, tags, etc.) is merged into its corresponding Delta table.<br>
Deletes and updates are handled via MERGE INTO SQL.<br>

Delta Lake → Analytics<br>
Delta tables are queryable by downstream denormalization pipeline and other consumers<br>


### Tech Stack ###

Python – Threading – concurrency & batch synchronization
Apache Spark (Structured Streaming)<br>
Apache Kafka<br>
Delta Lake<br>
Debezium (for CDC events)<br>
S3/Cloud Object Storage – durable and scalable table storage
