const http = require('http');
const unzipper = require('unzipper');
const fs = require('fs');
const https = require('https');
const aws = require('aws-sdk');
const { v4: uuidv4 } = require('uuid');
const crypto = require('crypto');
const Magic = require('mmmagic').Magic;
const util = require('util');
const { parse } = require('querystring');
const multiparty = require('multiparty');
const { fork } = require('child_process');
const path = require('path');

const getBuild = (owner, repo, commit = undefined) => new Promise((resolve, reject) => {
    // todo: uuid
    const dir = `/tmp/${Date.now()}`;

    const commitString = commit ? '/' + commit : '';
    const file = fs.createWriteStream(dir + '.zip');
    const zlib = require('zlib');
    const thing = `https://codeload.github.com/${owner}/${repo}/zip${commitString}`;
    https.get(thing, (_response) => {
	    const stream = _response.pipe(unzipper.Extract({ path: dir }));
            stream.on('finish', () => {
                fs.readdir(dir, (err, files) => {
	            resolve(dir + '/' + files[0]);
                });
            });
	});
});

const getCommit = (owner, repo, commit = undefined) => new Promise((resolve, reject) => {
    const _headers = {
        'User-Agent': 'HomegamesLandlord/0.1.0'
    };

    https.get({
        hostname: 'api.github.com',
        path: `/repos/${owner}/${repo}${commit ? '/' + commit : ''}`,
        headers: _headers
    }, res => {
        
        let _buf = '';

        res.on('end', () => {
            resolve(_buf);
        });

        res.on('data', (_data) => {
            _buf += _data;
        }); 

    });
});

// 50 MB max
const MAX_SIZE = 50 * 1024 * 1024;

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

const createRecord = (developerId, assetId, size, metadata) => new Promise((resolve, reject) => {
    const client = new aws.DynamoDB({region: 'us-west-2'});
    const params = {
        TableName: 'homegames_assets',
        Item: {
            'developer_id': {S: developerId},
            'asset_id': {S: assetId},
            'created_at': {N: '' + Date.now()},
            'metadata': {S: JSON.stringify(metadata)},
            'status': {S: 'created'},
            'size': {N: '' + size}
        }
    };

    client.putItem(params, (err, putResult) => {
        if (!err) {
            resolve();
        } else {
            reject(err);
        }
    });

});

const getOwnerEmail = (owner) => new Promise((resolve, reject) => {
     const _headers = {
        'User-Agent': 'HomegamesLandlord/0.1.0',
        'Authorization': 'Basic ' + Buffer.from('prosif' + ':' + 'whoops').toString('base64')
    };

    https.get({
        hostname: 'api.github.com',
        path: `/users/${owner}`,
        headers: _headers
    }, res => {
        
        let _buf = '';

        res.on('end', () => {
            const data = JSON.parse(_buf);
            resolve(data.email);
        });

        res.on('data', (_data) => {
            _buf += _data;
        }); 

    });
});

const emailOwner = (owner) => new Promise((resolve, reject) => {
    getOwnerEmail(owner).then(_email => {
        const ses = new aws.SES({region: 'us-west-2'});
        const params = {
            Destination: {
                ToAddresses: [
                    _email
                ]
            },
            Message: {
                Body: {
                    Html: {
                        Charset: 'UTF-8',
                        Data: '<html><body>hello world</body></html>'
                    }   
                },
                Subject: {
                    Charset: 'UTF-8',
                    Data: 'Testing'
                }
            },
            Source: 'landlord@homegames.io'
        };
        ses.sendEmail(params, (err, data) => {
            err ? reject(err) : resolve(data);
        });
    });
});

const testGame = (game) => new Promise((resolve, reject) => {
    const instance = new game();
    // todo: make this good
    if (instance.getRoot()) {
        resolve();
    } else {
        reject();
    }
});

const getGameInstance = (owner, repo, commit) => new Promise((resolve, reject) => {

    getCommit(owner, repo, commit).then(_res => {

        getBuild(owner, repo, commit).then((dir) => {
            const sourceNodeModules = 'INSERTHERE';
            fs.symlink(sourceNodeModules, dir + '/node_modules', 'dir', (err) => {
                if (!err) {
                    const _game = require(dir);
                    resolve(_game);
                } else {
                    reject(err);
                }
            });
        });
    
    });

});

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
        const gamePublishRegex = new RegExp('/games/(\\S*)/publish');
        if (req.url.match(gamePublishRegex)) {
            getReqBody(req, (_data) => {
                const data = JSON.parse(_data);
                console.log(data);
                console.log('want to publish game');
                const _gamePublishRegex = new RegExp('/games/(\\S*)/publish');
                const gameId = _gamePublishRegex.exec(req.url)[1];
                getGameInstance(data.owner, data.repo, data.commit).then(game => {
                    testGame(game).then(() => {
                        emailOwner(data.owner)
                    }).then(() => {
                        res.end('emailed owner!');
                    });
                });
            });
        } else if (req.url === '/asset') {
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


                    let hack = false;

                    const uploadedFiles = fileValues[0].map(f => {

                        if (hack) {
                            return;
                        }

                        hack = true;

                        if (f.size > MAX_SIZE) {
                            res.writeHead(400);
                            res.end('File size exceeds ' + MAX_SIZE + ' bytes');
                        } else {
                            const assetId = getHash(uuidv4());

                            createRecord('joseph', assetId, f.size, {
                                'Content-Type': f.headers['content-type']
                            }).then(() => {

                                const childSession = fork(path.join(__dirname, 'upload.js'), 
                                    [
                                        `--path=${f.path}`, 
                                        `--developer=joseph`, 
                                        `--id=${assetId}`, 
                                        `--name=${f.originalFilename}`, 
                                        `size=${f.size}`, 
                                        `type=${f.headers['content-type']}`
                                    ]
                                );
                                res.writeHead(200, {'Content-Type': 'application/json'});
                                res.end(JSON.stringify({
                                    assetId
                                }))

                                return {
                                    path: f.path,
                                    size: f.size,
                                    name: f.originalFilename,
                                    contentType: f.headers['content-type']
                                }
                            });
                        }
                    });

                    //todo: make this a separate thing
//                    res.writeHead(200, {'Content-Type': 'application/json'});
//                    res.end(JSON.stringify(uploadedFiles))
                });
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

                console.log('got data');
                //https://api.github.com/repos/prosif/do-dad/git/commits/cd1b383cfe22a0bfcea13a05115bf9e735b3f5b1
                console.log(_data);
//                const commitString = data.github.commit ? '/' + data.github.commit : '';
                //const codeUrl = `https://api.github.com/repos/${data.github.owner}/${data.github.repo}/git/commits/${commitString}`
                //console.log(codeUrl);
                //getCommit(data.github.owner, data.github.repo, data.github.commit).then(_res => {
                //    getBuild(data.github.owner, data.github.repo, data.github.commit).then(() => {
                //        console.log('got code!!!!!');
                //    });
                //});

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

