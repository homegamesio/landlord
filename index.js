const http = require('http');

const server = http.createServer((req, res) => {
    res.end('insecure :( but working');
});
    
server.listen(80);

