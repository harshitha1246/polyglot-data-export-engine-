const assert = require('assert/strict');
const http = require('http');

const HOST = 'localhost';
const PORT = 8080;

function request({ method, path, body }) {
  return new Promise((resolve, reject) => {
    const payload = body ? JSON.stringify(body) : null;
    const req = http.request(
      {
        hostname: HOST,
        port: PORT,
        path,
        method,
        headers: payload
          ? {
              'Content-Type': 'application/json',
              'Content-Length': Buffer.byteLength(payload)
            }
          : undefined
      },
      (res) => {
        let data = '';
        res.on('data', (chunk) => {
          data += chunk;
        });
        res.on('end', () => {
          resolve({
            status: res.statusCode,
            headers: res.headers,
            body: data
          });
        });
      }
    );

    req.on('error', reject);
    if (payload) req.write(payload);
    req.end();
  });
}

function baseColumns() {
  return [
    { source: 'id', target: 'id' },
    { source: 'name', target: 'name' },
    { source: 'value', target: 'value' },
    { source: 'metadata', target: 'metadata' }
  ];
}

async function createExportJob(format, compression) {
  const payload = { format, columns: baseColumns() };
  if (compression) payload.compression = compression;

  const response = await request({ method: 'POST', path: '/exports', body: payload });
  assert.equal(response.status, 201, `expected 201 when creating ${format} job`);

  const parsed = JSON.parse(response.body);
  assert.equal(parsed.status, 'pending');
  assert.ok(parsed.exportId);
  return parsed.exportId;
}

function probeDownload(exportId, expectedHeaders = {}) {
  return new Promise((resolve, reject) => {
    const req = http.get(
      {
        hostname: HOST,
        port: PORT,
        path: `/exports/${exportId}/download`
      },
      (res) => {
        let collected = Buffer.alloc(0);
        let done = false;

        const finish = () => {
          if (done) return;
          done = true;
          resolve({
            status: res.statusCode,
            headers: res.headers,
            firstChunk: collected.toString('utf8')
          });
        };

        res.on('data', (chunk) => {
          if (collected.length < 2048) {
            collected = Buffer.concat([collected, chunk]);
          }

          if (collected.length >= 512) {
            finish();
            req.destroy();
            res.destroy();
          }
        });

        res.on('end', finish);
        res.on('close', finish);
        res.on('error', (error) => {
          if (error && (error.code === 'ECONNRESET' || error.message === 'aborted')) {
            finish();
            return;
          }
          reject(error);
        });

        for (const [name, value] of Object.entries(expectedHeaders)) {
          assert.equal(res.headers[name], value, `header ${name} mismatch`);
        }
      }
    );

    req.on('error', (error) => {
      if (error.code === 'ECONNRESET') return;
      reject(error);
    });
  });
}

async function run() {
  const invalid = await request({
    method: 'POST',
    path: '/exports',
    body: { format: 'yaml', columns: baseColumns() }
  });
  assert.equal(invalid.status, 400);

  const csvId = await createExportJob('csv', 'gzip');
  const csvProbe = await probeDownload(csvId, {
    'content-type': 'text/csv',
    'content-encoding': 'gzip'
  });
  assert.equal(csvProbe.status, 200);

  const jsonId = await createExportJob('json', 'gzip');
  const jsonProbe = await probeDownload(jsonId, {
    'content-type': 'application/json',
    'content-encoding': 'gzip'
  });
  assert.equal(jsonProbe.status, 200);

  const xmlId = await createExportJob('xml', 'gzip');
  const xmlProbe = await probeDownload(xmlId, {
    'content-type': 'application/xml',
    'content-encoding': 'gzip'
  });
  assert.equal(xmlProbe.status, 200);

  const parquetId = await createExportJob('parquet');
  const parquetProbe = await probeDownload(parquetId, {
    'content-type': 'application/x-parquet'
  });
  assert.equal(parquetProbe.status, 200);

  const notFound = await request({ method: 'GET', path: '/exports/not-found/download' });
  assert.equal(notFound.status, 404);

  const benchmark = await request({ method: 'GET', path: '/exports/benchmark?rows=1000' });
  assert.equal(benchmark.status, 200);
  const benchmarkBody = JSON.parse(benchmark.body);
  assert.equal(benchmarkBody.benchmarkRowCount, 1000);
  assert.equal(benchmarkBody.results.length, 4);

  for (const item of benchmarkBody.results) {
    assert.ok(['csv', 'json', 'xml', 'parquet'].includes(item.format));
    assert.ok(Number.isFinite(item.durationSeconds));
    assert.ok(Number.isInteger(item.fileSizeBytes));
    assert.ok(Number.isInteger(item.peakMemoryMB));
    assert.ok(Number.isInteger(item.rowsProcessed));
  }

  console.log('All integration checks passed.');
}

run().catch((error) => {
  console.error(error);
  process.exit(1);
});
