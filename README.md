# Polyglot Data Export Engine

A high-performance, memory-efficient export engine capable of streaming millions of database rows concurrently into CSV, JSON, XML and Parquet formats.

## Overview

This repository contains a Node.js application that connects to PostgreSQL and provides HTTP endpoints for:

- creating export jobs (`POST /exports`)
- downloading results with streaming support (`GET /exports/:id/download`)
- running benchmarks (`GET /exports/benchmark`)

The service is containerized using Docker and orchestrated with `docker-compose`. A seeding script populates the database with 10 million rows automatically.

## Getting Started

1. **Clone repository**
   ```bash
   git clone <repo> polyglot-data-export-engine
   cd polyglot-data-export-engine
   ```
2. **Copy environment template**
   ```bash
   cp .env.example .env
   ```
3. **Start services**
   ```bash
   docker-compose up --build
   ```
   The first run will take a few minutes while PostgreSQL starts and seeds the `records` table with 10,000,000 rows.

4. **Verify database**
   ```bash
   docker exec -it $(docker ps -qf "name=polyglot-data-export-engine_db") psql -U user -d exports_db -c "SELECT COUNT(*) FROM records;"
   ```
   Should return `10000000`.

## API

### Create export job

`POST /exports`

```json
{
  "format": "csv|json|xml|parquet",
  "columns": [
    {"source": "name", "target": "username"},
    {"source": "value", "target": "amount"},
    {"source": "metadata", "target": "metadata"}
  ],
  "compression": "gzip"  // optional for text formats
}
```

Response 201:

```json
{ "exportId": "<uuid>", "status": "pending" }
```

### Download export

`GET /exports/{exportId}/download`

- Streams the data in the requested format
- Sets appropriate `Content-Type` and `Content-Disposition` headers
- Supports gzip compression for CSV/JSON/XML

### Benchmark

`GET /exports/benchmark` returns a JSON report with metrics for each format.

## Implementation Details

- Uses `pg-query-stream` to read database rows in a cursor and pipe to output
- Serialization logic is format-specific with strategies to flatten/serialize nested JSON
- `init-db.sh` creates the `records` table and populates 10M rows on container startup
- The app enforces constant memory usage by streaming and limiting container memory to 256MB

## Project Structure

```
./
├── Dockerfile
├── docker-compose.yml
├── .env.example
├── init-db.sh
├── package.json
└── src/
    ├── index.js
    └── db.js
```

## Notes

- This is a demonstrative implementation. In production you would persist export jobs to a database and provide status updates.
- The benchmark endpoint currently returns stub metrics; you can extend it to measure export time, file size and memory usage as needed.

## License

MIT
