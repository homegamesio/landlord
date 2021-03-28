const http = require('http');
const aws = require('aws-sdk');
const { v4: uuidv4 } = require('uuid');
const crypto = require('crypto');
const Magic = require('mmmagic').Magic;
const util = require('util');
const { parse } = require('querystring');
const multiparty = require('multiparty');
const { fork } = require('child_process');
const path = require('path');

const getHash = (input) => {
    return crypto.createHash('md5').update(input).digest('hex');
};

const generateGameId = () => getHash(uuidv4());

const getReqBody = (req, cb) => {
    let _body = '';
    req.on('data', chunk => {
        _body += chunk.toString();
    });

    req.on('end', () => {
        cb && cb(_body);
    });
};

const server = http.createServer((req, res) => {
    if (req.method === 'GET') {
        if (req.url === '/games') {
            const client = new aws.DynamoDB.DocumentClient({region: 'us-west-2'});
            const params = {
                TableName: 'hg_games',
                KeyConditionExpression: '#devId = :devId',
                ExpressionAttributeNames: {
                    '#devId': 'developer_id'
                },
                ExpressionAttributeValues: {
                    ':devId': 'joseph'
                }
            };

            client.query(params, (err, data) => {
                if (err) {
                    res.end(err);
                } else {
                    res.writeHead(200, {
                        'Content-Type': 'application/json'
                    });
 
                    res.end(JSON.stringify({
                        games: data.Items
                    }));
                }
            });
        } else {
            res.end('ok');
        }
    } else if (req.method === 'POST') {
        if (req.url === '/asset') {
            const maxSize = 1024 * 50;
//            let _body = '';
//            req.on('data', chunk => {
//                _body += chunk.toString();
//                if (_body.length > maxSize) {
//                    res.end('2 big');
//                } 
//            });
//
//            req.on('end', () => {
                const form = new multiparty.Form();
                form.parse(req, (err, fields, files) => {
                    const fileValues = Object.values(files);

                    const uploadedFiles = fileValues[0].map(f => {
                        console.log('uufuff');
                        console.log(f);

                        const assetId = getHash(uuidv4());
                        const childSession = fork(path.join(__dirname, 'upload.js'), 
                            [`--path=${f.path}`, `--id=${assetId}`, `--name=${f.originalFilename}`, `size=${f.size}`, `type=${f.headers['content-type']}`]
                        );

                        return {
                            path: f.path,
                            size: f.size,
                            name: f.originalFilename,
                            contentType: f.headers['content-type']
                        }
                    });

                    //todo: make this a separate thing
                    console.log('dfgdfg');

                    console.log(uploadedFiles);
    
                    res.writeHead(200, {'Content-Type': 'application/json'});
                    res.end(JSON.stringify(uploadedFiles))
                });
//                let thing = _body.toString();
//                thing = thing.split('&');
//                for (let i = 0; i < thing.length; i++) {
//                    const _d = thing[i].split("=");
//                    tings[_d[0]] = thing[1];
//                }
//                console.log(tings);
                // 50 MB limit. todo: know if we already ended the req because of limit
//                cfcff();
//                const magic = new Magic();
//                console.log(parse(_body))
//                const fs = require('fs');
//                fs.writeFileSync("/Users/josephgarcia/datting.jpg", _body);
//                magic.detect(Buffer.from(_body, 'utf-8'), (err, result) => {
//
//                    console.log(err);
//                    console.log(result);
//                    res.end('nice bruv');
//                });
//            });

        } else if (req.url === '/games') {
            getReqBody(req, (_data) => {
                const data = JSON.parse(_data);

                const client = new aws.DynamoDB({region: 'us-west-2'});
                const readClient = new aws.DynamoDB.DocumentClient({region: 'us-west-2'});

                const params = {
                    TableName: 'hg_games',
                    IndexName: 'game_name_index',
                    KeyConditionExpression: '#devId = :devId and #gameName = :gameName',
                    ExpressionAttributeNames: {
                        '#devId': 'developer_id',
                        '#gameName': 'game_name'
                    },
                    ExpressionAttributeValues: {
                        ':devId': data.developer_id,
                        ':gameName': data.game_name
                    }
                };

                readClient.query(params, (err, result) => {
                    if (err) {
                        res.end(err);
                    } else {
                        if (result.Items.length) {
                            console.log(result);
                            res.writeHead(400);
                            res.end('that already exists');
                        } else {
                            const gameId = generateGameId();
                            const params = {
                                TableName: 'hg_games',
                                Item: {
                                    'game_id': {S: gameId},
                                    'developer_id': {S: data.developer_id},
                                    'game_name': {S: data.game_name},
                                    'created_at': {N: '' + Date.now()}
                                }
                            };

                            client.putItem(params, (err, putResult) => {

                                console.log(err);
                                console.log(putResult);
                                res.writeHead(200, {
                                    'Content-Type': 'application/json'
                                });
                                res.end(JSON.stringify(params.Item));
                            });
                        }
                    }
                });
            });
        }
    } else {
        res.end('not found');
    }
});
    
server.listen(80);

