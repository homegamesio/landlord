const http = require('http');
const https = require('https');
const fs = require('fs');

const HTTPS_ENABLED = false;

if (HTTPS_ENABLED) {
    
    const options = {};

    const server = https.createServer(options, (req, res) => {
        res.end('ok');
    });
    
    server.listen(443);

} else { 
    
    const server = http.createServer((req, res) => {
        res.end('insecure :(');
    });
    
    server.listen(80);
}

