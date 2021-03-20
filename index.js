const http = require('http');
const aws = require('aws-sdk');
const { v4: uuidv4 } = require('uuid');
const crypto = require('crypto');

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
        if (req.url === '/games') {
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

