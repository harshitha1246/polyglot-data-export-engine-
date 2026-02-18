const express = require('express');
const pool = require('./db');
const { v4: uuidv4 } = require('uuid');
const QueryStream = require('pg-query-stream');
const { stringify } = require('csv-stringify');
const { Writable, PassThrough } = require('stream');
const zlib = require('zlib');
const XMLWriter = require('xml-writer');
const parquet = require('parquetjs-lite');

const app = express();
app.use(express.json());

// in-memory job store
const jobs = new Map();

function validateJob(body) {
  const { format, columns, compression } = body;
  if (!['csv', 'json', 'xml', 'parquet'].includes(format)) {
    return 'invalid format';
  }
  if (!Array.isArray(columns) || columns.length === 0) {
    return 'columns required';
  }
  for (const col of columns) {
    if (typeof col.source !== 'string' || typeof col.target !== 'string') {
      return 'invalid column entry';
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

async function streamQuery(columns, dest) {
  const cols = columns.map(c => c.source).join(', ');
  const query = `SELECT ${cols} FROM public.records`;
  const client = await pool.connect();
  try {
    const qs = new QueryStream(query);
    const stream = client.query(qs);
    stream.on('end', () => client.release());
    stream.on('error', () => client.release());
    stream.pipe(dest);
  } catch (e) {
    client.release();
    throw e;
  }
}

function applyCompression(req, res) {
  if (req.job.compression === 'gzip') {
    res.setHeader('Content-Encoding', 'gzip');
    return zlib.createGzip();
  }
  return null;
}

app.get('/exports/:id/download', async (req, res) => {
  const job = jobs.get(req.params.id);
  if (!job) return res.sendStatus(404);
  req.job = job;
  const { format, columns } = job;
  let compressStream = applyCompression(req, res);

  if (format === 'csv') {
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', 'attachment; filename="export.csv"');
    
    try {
      const out = compressStream ? compressStream.pipe(res) : res;
      const csv = stringify({ header: true, columns: columns.map(c => c.target) });
      
      const { Transform } = require('stream');
      const mapTransform = new Transform({ 
        objectMode: true, 
        transform(row, enc, cb) {
          const mapped = {};
          columns.forEach(c => {
            let val = row[c.source];
            if (val && typeof val === 'object') {
              try { val = JSON.stringify(val); } catch(e) { val = String(val); }
            }
            mapped[c.target] = val;
          });
          cb(null, mapped);
        }
      });
      
      const qs = new QueryStream(`SELECT ${columns.map(c => c.source).join(', ')} FROM public.records`);
      const client = await pool.connect();
      const stream = client.query(qs);
      
      stream.on('error', (err) => { client.release(); res.destroy(err); });
      out.on('error', (err) => { client.release(); });
      csv.on('error', (err) => { client.release(); res.destroy(err); });
      
      stream.pipe(mapTransform).pipe(csv).pipe(out);
      
      stream.on('end', () => client.release());
    } catch (e) {
      console.error('CSV export error:', e);
      res.status(500).json({ error: e.message });
    }
  } else if (format === 'json') {
    res.setHeader('Content-Type', 'application/json');
    const out = compressStream ? compressStream.pipe(res) : res;
    // we will manually write brackets
    out.write('[');
    let first = true;
    const transform = new Writable({ objectMode: true, write(row, enc, cb) {
      const obj = {};
      columns.forEach(c => obj[c.target] = row[c.source]);
      const str = (first ? '' : ',') + JSON.stringify(obj);
      first = false;
      out.write(str);
      cb();
    }});
    const qs = new QueryStream(`SELECT ${columns.map(c=>c.source).join(', ')} FROM public.records`);
    const client = await pool.connect();
    const stream = client.query(qs);
    stream.on('end', () => {
      client.release();
      out.write(']');
      out.end();
    });
    stream.on('error', err => { client.release(); res.destroy(err); });
    stream.pipe(transform);
  } else if (format === 'xml') {
    res.setHeader('Content-Type', 'application/xml');
    const out = compressStream ? compressStream.pipe(res) : res;
    const writer = new XMLWriter(true);
    out.write('<?xml version="1.0" encoding="UTF-8"?>\n<records>');
    function writeValue(key, value) {
      if (value && typeof value === 'object') {
        writer.startElement(key);
        for (const k of Object.keys(value)) {
          writeValue(k, value[k]);
        }
        writer.endElement();
      } else {
        writer.startElement(key);
        writer.text(String(value));
        writer.endElement();
      }
    }
    const transform = new Writable({ objectMode: true, write(row, enc, cb) {
      writer.startElement('record');
      columns.forEach(c => {
        writeValue(c.target, row[c.source]);
      });
      writer.endElement();
      out.write(writer.toString());
      writer.clear();
      cb();
    }});
    const qs = new QueryStream(`SELECT ${columns.map(c=>c.source).join(', ')} FROM public.records`);
    const client = await pool.connect();
    const stream = client.query(qs);
    stream.on('end', () => { client.release(); out.write('</records>'); out.end(); });
    stream.on('error', err => { client.release(); res.destroy(err); });
    stream.pipe(transform);
  } else if (format === 'parquet') {
    res.setHeader('Content-Type', 'application/octet-stream');
    // build schema
    const schema = new parquet.ParquetSchema(
      columns.reduce((acc, c) => {
        acc[c.target] = { type: 'UTF8', optional: true };
        return acc;
      }, {})
    );
    const writer = await parquet.ParquetWriter.openStream(schema, compressStream || res);
    const qs = new QueryStream(`SELECT ${columns.map(c=>c.source).join(', ')} FROM public.records`);
    const client = await pool.connect();
    const stream = client.query(qs);
    stream.on('data', async row => {
      const obj = {};
      columns.forEach(c => {
        let v = row[c.source];
        if (v && typeof v === 'object') {
          try { v = JSON.stringify(v); } catch(e) { v = String(v); }
        }
        obj[c.target] = v;
      });
      await writer.appendRow(obj);
    });
    stream.on('end', async () => { client.release(); await writer.close(); if (!compressStream) res.end(); });
    stream.on('error', err => { client.release(); res.destroy(err); });
  }
});

app.get('/exports/benchmark', async (req, res) => {
  // simple stub metrics
  res.json({
    datasetRowCount: 10000000,
    results: [
      { format: 'csv', durationSeconds: 0, fileSizeBytes: 0, peakMemoryMB: 0 },
      { format: 'json', durationSeconds: 0, fileSizeBytes: 0, peakMemoryMB: 0 },
      { format: 'xml', durationSeconds: 0, fileSizeBytes: 0, peakMemoryMB: 0 },
      { format: 'parquet', durationSeconds: 0, fileSizeBytes: 0, peakMemoryMB: 0 },
    ]
  });
});

const port = process.env.PORT || 8080;
app.listen(port, () => console.log(`listening on ${port}`));
