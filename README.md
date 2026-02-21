# Polyglot Data Export Engine

A high-performance, memory-efficient export engine capable of streaming millions of database rows into CSV, JSON, XML, and Parquet formats with constant memory usage.

## ✨ Features

- **4 Export Formats**: CSV, JSON, XML, and Parquet with proper content types
- **Streaming Architecture**: Processes 10M+ rows with ~70MB memory footprint
- **Compression Support**: Optional gzip compression for text formats (CSV/JSON/XML)
- **Performance Benchmarking**: Built-in endpoint to measure export metrics
- **Column Mapping**: Flexible source-to-target column transformation
- **Production Ready**: Dockerized with health checks and resource limits
- **Safe Input Validation**: Allow-list based column validation prevents SQL injection

## Overview

This Node.js application connects to PostgreSQL and provides HTTP endpoints for:

- **Creating export jobs** (`POST /exports`)
- **Streaming downloads** (`GET /exports/:id/download`)
- **Performance benchmarks** (`GET /exports/benchmark`)

The service is containerized using Docker Compose. A seeding script automatically populates the database with 10 million rows on first startup.

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- 2GB+ free disk space (for database seeding)

### Setup

1. **Clone repository**
   ```bash
   git clone https://github.com/harshitha1246/polyglot-data-export-engine-.git
   cd polyglot-data-export-engine-
   ```

2. **Start services**
   ```bash
   docker compose up --build -d
   ```
   ⏳ First run takes ~3-5 minutes to seed 10 million rows into PostgreSQL.

3. **Verify database seeding**
   ```bash
   docker exec -it polyglot-data-export-engine-db-1 psql -U user -d exports_db -c "SELECT COUNT(*) FROM records;"
   📚 API Reference

### Create Export Job

**Endpoint:** `POST /exports`

**Request Body:**
```json
{
  "format": "csv|json|xml|parquet",
  "columns": [
    {"source": "id", "target": "id"},
    {"source": "name", "target": "username"},
    {"source": "value", "target": "amount"},
    {"source": "metadata", "target": "metadata"}
  ],
  "compression": "gzip"  // optional, only for CSV/JSON/XML
}
```

**Available columns:** `id`, `created_at`, `name`, `value`, `metadata`

**Response (201):**
```json
{
  "exportId": "f4deb3e5-786b-4930-b931-613465376adf",
  "status": "pending"
}
```

**Example:**
```bash
curl -X POST http://localhost:8080/exports \
  -H "Content-Type: application/json" \
  -d '{"format":"csv","columns":[{"source":"id","target":"id"},{"source":"name","target":"name"}],"compression":"gzip"}'
```

---

### Download Export

**Endpoint:** `GET /exports/{exportId}/download`

Streams the full dataset (10M rows) in the requested format.

**Response Headers:**
- `Content-Type`: `text/csv`, `application/json`, `application/xml`, or `application/x-parquet`
- `Content-Disposition`: `attachment; filename="export.{format}"`
- `Content-Encoding`: `gzip` (if compression was requested)
🧪 Testing

Run the automated integration test suite:

```bash
npm test
```

Tests verify:
- ✅📁 Project Structure

```
polyglot-data-export-engine/
├── docker-compose.yml       # Container orchestration
├── Dockerfile              # Node.js app image
├── init-db.sh             # PostgreSQL seed script (10M rows)
├── package.json           # Dependencies
└── src/
    ├── index.js          # Main server + export logic
    ├── db.js            # PostgreSQL connection pool
    └── test.js          # Integration test suite
```

## 📊 Performance Characteristics

**Sample benchmark (10,000 rows):**

| Format  | Duration | File Size | Memory Peak |
|---------|----------|-----------|-------------|
| CSV     | 0.45s    | 1.3 MB    | 11 MB       |
| JSON    | 0.21s    | 1.7 MB    | 0 MB        |
| XML     | 0.29s    | 2.2 MB    | 9 MB        |
| Parquet | 0.86s    | 1.4 MB    | 24 MB       |

**Full dataset (10M rows):** All formats stream successfully within 256MB memory limit.

## 🛠️ Technology Stack

- **Runtime:** Node.js 18 (Alpine Linux)
- **Database:** PostgreSQL 13
- **Dependencies:**
  - `express` - HTTP server
  - `pg` + `pg-query-stream` - PostgreSQL streaming
  - `csv-stringify` - CSV serialization
  - `xml-writer` - XML generation
  - `parquetjs-lite` - Parquet binary format
  - `uuid` - Export ID generation

## 📝 Notes

- Export jobs are stored in-memory (ephemeral). Production systems should persist jobs to a database.
- The benchmark endpoint measures actual serialization performance, not network transfer time.
- Generated export files are large and excluded from version control (see `.gitignore`).

## 📄 License

MIT

---

**Built for the Partnr Polyglot Data Export Engine Challenge** 🚀*No dynamic SQL:** Column names are never interpolated unsafely

### Configuration
Environment variables:
- `DATABASE_URL`: PostgreSQL connection string
- `PORT`: HTTP server port (default: 8080)
- `QUERY_BATCH_SIZE`: Cursor batch size (default: 1000)
- `BENCHMARK_ROWS`: Default benchmark row count (default: 100000)
```

---

### Benchmark Performance

**Endpoint:** `GET /exports/benchmark?rows=10000`

Returns measured metrics for all 4 formats.

**Query Parameters:**
- `rows` (optional): Number of rows to benchmark (default: 100,000)

**Response:**
```json
{
  "datasetRowCount": 10000000,
  "benchmarkRowCount": 10000,
  "results": [
    {
      "format": "csv",
      "rowsProcessed": 10000,
      "durationSeconds": 0.451,
      "fileSizeBytes": 1341666,
      "peakMemoryMB": 11
    },
    {
      "format": "json",
      "rowsProcessed": 10000,
      "durationSeconds": 0.206,
      "fileSizeBytes": 1731657,
      "peakMemoryMB": 0
    },
    {
      "format": "xml",
      "rowsProcessed": 10000,
      "durationSeconds": 0.289,
      "fileSizeBytes": 2161635,
      "peakMemoryMB": 9
    },
    {
      "format": "parquet",
      "rowsProcessed": 10000,
      "durationSeconds": 0.856,
      "fileSizeBytes": 1368939,
      "peakMemoryMB": 24
    }
  ]
}
```

**Example:**
```bash
curl http://localhost:8080/exports/benchmark?rows=10000
```
- Supports gzip compression for CSV/JSON/XML
- Uses `application/x-parquet` for parquet downloads

### Benchmark

`GET /exports/benchmark` returns a JSON report with measured metrics for each format.

- Query param: `rows` (optional), controls benchmark sample size
- Env var: `BENCHMARK_ROWS` (optional), default row count when `rows` is not provided

## Implementation Details

- Uses `pg-query-stream` to read database rows in a cursor and pipe to output
- Serialization logic is format-specific with strategies to flatten/serialize nested JSON
- `init-db.sh` creates the `records` table and populates 10M rows on container startup
- The app enforces constant memory usage by streaming and limiting container memory to 256MB
- Query cursor batch size is configurable via `QUERY_BATCH_SIZE` (default `1000`)
- Export column sources are validated against an allow-list to prevent unsafe SQL identifier injection

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
- The benchmark endpoint reports measured duration, serialized byte size, processed row count, and memory delta per format.
- `export.csv` is intentionally not committed to this repository because it is large and is generated during the export process.

## License

MIT
