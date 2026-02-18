// simple smoke tests using node's http
const http = require('http');

function postExport(format) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify({
      format,
      columns: [
        { source: 'id', target: 'id' },
        { source: 'name', target: 'name' },
        { source: 'metadata', target: 'metadata' }
      ]
    });
    const opts = {
      hostname: 'localhost', port: 8080, path: '/exports', method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Content-Length': data.length }
    };
    const req = http.request(opts, res => {
      let body = '';
      res.on('data', c => body += c);
      res.on('end', () => resolve({status: res.statusCode, body}));
    });
    req.on('error', reject);
    req.write(data);
    req.end();
  });
}

async function run() {
  console.log('posting export job for csv');
  console.log(await postExport('csv'));
  console.log('fetching benchmark');
  http.get('http://localhost:8080/exports/benchmark', res => {
    let b = '';
    res.on('data', c => b += c);
    res.on('end', () => console.log('benchmark', b));
  });
}
run();
