const express = require('express');
const pool = require('./db');
const { v4: uuidv4 } = require('uuid');
const QueryStream = require('pg-query-stream');
const { stringify } = require('csv-stringify');
const { Writable, PassThrough, Transform } = require('stream');
const { pipeline } = require('stream/promises');
const zlib = require('zlib');
const XMLWriter = require('xml-writer');
const parquet = require('parquetjs-lite');

const app = express();
app.use(express.json());

// in-memory job store
const jobs = new Map();
const ALLOWED_COLUMNS = new Set(['id', 'created_at', 'name', 'value', 'metadata']);
const IDENTIFIER_PATTERN = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

function isValidIdentifier(value) {
  return typeof value === 'string' && IDENTIFIER_PATTERN.test(value);
}

function validateJob(body) {
  const { format, columns, compression } = body;
  if (!['csv', 'json', 'xml', 'parquet'].includes(format)) {
    return 'invalid format';
  }
  if (!Array.isArray(columns) || columns.length === 0) {
    return 'columns required';
  }
  for (const col of columns) {
    if (!isValidIdentifier(col.source) || !isValidIdentifier(col.target)) {
      return 'invalid column entry';
    }
    if (!ALLOWED_COLUMNS.has(col.source)) {
      return `unsupported source column: ${col.source}`;
    }
  }
  if (compression && compression !== 'gzip') {
    return 'invalid compression';
  }
  if (compression && format === 'parquet') {
    return 'compression not supported for parquet';
  }
  return null;
}

app.post('/exports', (req, res) => {
  const err = validateJob(req.body);
  if (err) return res.status(400).json({ error: err });
  const id = uuidv4();
  jobs.set(id, { id, status: 'pending', ...req.body });
  res.status(201).json({ exportId: id, status: 'pending' });
});

function serializeValue(value) {
  if (value && typeof value === 'object') {
    try {
      return JSON.stringify(value);
    } catch (e) {
      return String(value);
    }
  }
  return value;
}

function mapRow(columns, row) {
  const mapped = {};
  for (const col of columns) {
    mapped[col.target] = serializeValue(row[col.source]);
  }
  return mapped;
}

function buildSelectQuery(columns, limit) {
  const cols = columns.map(c => c.source).join(', ');
  const limitClause = Number.isInteger(limit) && limit > 0 ? ` LIMIT ${limit}` : '';
  return `SELECT ${cols} FROM public.records${limitClause}`;
}

function createQueryStream(query) {
  const batchSize = Number.parseInt(process.env.QUERY_BATCH_SIZE, 10) || 1000;
  return new QueryStream(query, [], { batchSize });
}

function buildXmlRecord(mapped) {
  const writer = new XMLWriter(true);
  writer.startElement('record');
  for (const key of Object.keys(mapped)) {
    writer.startElement(key);
    writer.text(String(mapped[key] ?? ''));
    writer.endElement();
  }
  writer.endElement();
  return writer.toString();
}

function createJsonTransform(columns) {
  let first = true;
  return new Transform({
    objectMode: true,
    transform(row, enc, cb) {
      const body = JSON.stringify(mapRow(columns, row));
      cb(null, first ? `[${body}` : `,${body}`);
      first = false;
    },
    final(cb) {
      cb(null, first ? '[]' : ']');
    }
  });
}

function createXmlTransform(columns) {
  let started = false;
  return new Transform({
    objectMode: true,
    transform(row, enc, cb) {
      const chunk = buildXmlRecord(mapRow(columns, row));
      if (!started) {
        started = true;
        cb(null, `<?xml version="1.0" encoding="UTF-8"?>\n<records>${chunk}`);
        return;
      }
      cb(null, chunk);
    },
    final(cb) {
      cb(null, started ? '</records>' : '<?xml version="1.0" encoding="UTF-8"?>\n<records></records>');
    }
  });
}

function createCsvPipeline(columns) {
  const mapper = new Transform({
    objectMode: true,
    transform(row, enc, cb) {
      cb(null, mapRow(columns, row));
    }
  });
  const csv = stringify({ header: true, columns: columns.map(c => c.target) });
  return [mapper, csv];
}

function contentTypeForFormat(format) {
  if (format === 'csv') return 'text/csv';
  if (format === 'json') return 'application/json';
  if (format === 'xml') return 'application/xml';
  return 'application/x-parquet';
}

function extensionForFormat(format) {
  return format === 'parquet' ? 'parquet' : format;
}

async function streamTextExport(format, columns, dest) {
  const query = buildSelectQuery(columns);
  const client = await pool.connect();

  try {
    const stream = client.query(createQueryStream(query));

    if (format === 'csv') {
      const [mapper, csv] = createCsvPipeline(columns);
      await pipeline(stream, mapper, csv, dest);
      return;
    }

    if (format === 'json') {
      await pipeline(stream, createJsonTransform(columns), dest);
      return;
    }

    await pipeline(stream, createXmlTransform(columns), dest);
  } finally {
    client.release();
  }
}

function createByteCounter() {
  const counter = {
    bytes: 0,
    stream: new Writable({
      write(chunk, enc, cb) {
        counter.bytes += Buffer.byteLength(chunk);
        cb();
      }
    })
  };
  return counter;
}

async function benchmarkTextFormat(format, columns, rowLimit) {
  const query = buildSelectQuery(columns, rowLimit);
  const client = await pool.connect();
  const counter = createByteCounter();
  const started = process.hrtime.bigint();
  const rssStart = process.memoryUsage().rss;
  let rowCount = 0;

  try {
    const stream = client.query(createQueryStream(query));

    if (format === 'csv') {
      const [mapper, csv] = createCsvPipeline(columns);
      mapper.on('data', () => {
        rowCount += 1;
      });
      await pipeline(stream, mapper, csv, counter.stream);
    }

    if (format === 'json') {
      const counting = new Transform({
        objectMode: true,
        transform(row, enc, cb) {
          rowCount += 1;
          cb(null, row);
        }
      });
      await pipeline(stream, counting, createJsonTransform(columns), counter.stream);
    }

    if (format === 'xml') {
      const counting = new Transform({
        objectMode: true,
        transform(row, enc, cb) {
          rowCount += 1;
          cb(null, row);
        }
      });
      await pipeline(stream, counting, createXmlTransform(columns), counter.stream);
    }

    const durationSeconds = Number(process.hrtime.bigint() - started) / 1e9;
    const peakMemoryMB = Math.max(0, Math.round((process.memoryUsage().rss - rssStart) / (1024 * 1024)));
    return {
      format,
      rowsProcessed: rowCount,
      durationSeconds: Number(durationSeconds.toFixed(3)),
      fileSizeBytes: counter.bytes,
      peakMemoryMB
    };
  } finally {
    client.release();
  }
}

async function benchmarkParquet(columns, rowLimit) {
  const query = buildSelectQuery(columns, rowLimit);
  const client = await pool.connect();
  const started = process.hrtime.bigint();
  const rssStart = process.memoryUsage().rss;
  const counter = createByteCounter();
  const passthrough = new PassThrough();
  passthrough.pipe(counter.stream);

  const schema = new parquet.ParquetSchema(
    columns.reduce((acc, c) => {
      acc[c.target] = { type: 'UTF8', optional: true };
      return acc;
    }, {})
  );

  let rowCount = 0;
  const writer = await parquet.ParquetWriter.openStream(schema, passthrough);

  try {
    const stream = client.query(createQueryStream(query));
    for await (const row of stream) {
      rowCount += 1;
      await writer.appendRow(mapRow(columns, row));
    }

    await writer.close();
    if (!counter.stream.writableFinished) {
      await new Promise((resolve, reject) => {
        counter.stream.on('finish', resolve);
        counter.stream.on('error', reject);
      });
    }

    const durationSeconds = Number(process.hrtime.bigint() - started) / 1e9;
    const peakMemoryMB = Math.max(0, Math.round((process.memoryUsage().rss - rssStart) / (1024 * 1024)));
    return {
      format: 'parquet',
      rowsProcessed: rowCount,
      durationSeconds: Number(durationSeconds.toFixed(3)),
      fileSizeBytes: counter.bytes,
      peakMemoryMB
    };
  } finally {
    client.release();
  }
}

function applyCompression(job, res) {
  if (job.compression === 'gzip') {
    res.setHeader('Content-Encoding', 'gzip');
    return zlib.createGzip();
  }
  return null;
}

app.get('/exports/:id/download', async (req, res) => {
  const job = jobs.get(req.params.id);
  if (!job) return res.sendStatus(404);
  const { format, columns } = job;

  res.setHeader('Content-Type', contentTypeForFormat(format));
  res.setHeader('Content-Disposition', `attachment; filename="export.${extensionForFormat(format)}"`);

  let out = res;
  const compressStream = applyCompression(job, res);
  if (compressStream) {
    compressStream.pipe(res);
    out = compressStream;
  }

  try {
    if (format === 'csv' || format === 'json' || format === 'xml') {
      await streamTextExport(format, columns, out);
      return;
    }

    const schema = new parquet.ParquetSchema(
      columns.reduce((acc, c) => {
        acc[c.target] = { type: 'UTF8', optional: true };
        return acc;
      }, {})
    );

    const writer = await parquet.ParquetWriter.openStream(schema, out);
    const query = buildSelectQuery(columns);
    const client = await pool.connect();
    const stream = client.query(createQueryStream(query));

    try {
      for await (const row of stream) {
        await writer.appendRow(mapRow(columns, row));
      }
      await writer.close();
    } catch (err) {
      out.destroy(err);
    } finally {
      client.release();
    }
  } catch (error) {
    if (!res.headersSent) {
      res.status(500).json({ error: error.message });
      return;
    }
    res.destroy(error);
  }
});

app.get('/exports/benchmark', async (req, res) => {
  const benchmarkColumns = [
    { source: 'id', target: 'id' },
    { source: 'name', target: 'name' },
    { source: 'value', target: 'value' },
    { source: 'metadata', target: 'metadata' }
  ];

  const rowLimit = Number.parseInt(req.query.rows, 10) || Number.parseInt(process.env.BENCHMARK_ROWS, 10) || 100000;

  try {
    const csv = await benchmarkTextFormat('csv', benchmarkColumns, rowLimit);
    const json = await benchmarkTextFormat('json', benchmarkColumns, rowLimit);
    const xml = await benchmarkTextFormat('xml', benchmarkColumns, rowLimit);
    const parquetResult = await benchmarkParquet(benchmarkColumns, rowLimit);

    res.json({
      datasetRowCount: 10000000,
      benchmarkRowCount: rowLimit,
      results: [csv, json, xml, parquetResult]
    });
  } catch (error) {
    res.status(500).json({ error: `benchmark failed: ${error.message}` });
  }
});

const port = process.env.PORT || 8080;
app.listen(port, () => console.log(`listening on ${port}`));
