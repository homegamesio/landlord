const http = require('http');
const https = require('https');
const unzipper = require('unzipper');
const url = require('url');
const archiver = require('archiver');
const fs = require('fs');
const aws = require('aws-sdk');
const { v4: uuidv4 } = require('uuid');
const crypto = require('crypto');
const Magic = require('mmmagic').Magic;
const util = require('util');
const { parse } = require('querystring');
const multiparty = require('multiparty');
const { fork } = require('child_process');
const path = require('path');
const { verifyAccessToken } = require('homegames-common');

const getBuild = (owner, repo, commit = undefined) => new Promise((resolve, reject) => {
    // todo: uuid
    const dir = `/tmp/${Date.now()}`;

    const commitString = commit ? '/' + commit : '';
    const file = fs.createWriteStream(dir + '.zip');
    const zlib = require('zlib');
    const thing = `https://codeload.github.com/${owner}/${repo}/zip${commitString}`;
    const archive = archiver('zip');
    const output = fs.createWriteStream(dir + 'out.zip');

    https.get(thing, (_response) => {
        output.on('close', () => {
            fs.readdir(dir, (err, files) => {
                resolve({
                    path: dir + '/' + files[0],
                    zipPath: dir + 'out.zip'
                });
            });
        });
    
        const stream = _response.pipe(unzipper.Extract({ path: dir }));

        stream.on('finish', () => {
            archive.directory(dir, false);
            archive.finalize();
        });

        archive.pipe(output);
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

const getGame = (gameId, version = null) => new Promise((resolve, reject) => {

    if (version) {
        const client = new aws.DynamoDB({region: 'us-west-2'});

        const params = {
            TableName: 'hg_games',
            Key: {
                'game_id': {
                    S: gameId
                }
            }
        };

        client.getItem(params, (err, result) => {
            if (err) {
                reject(err.toString());
            } else {
                if (result.Item) {
                    resolve(mapGame(result.Item));
                } else {
                    reject('No results');
                }
            }
        });
    } else {
        const readClient = new aws.DynamoDB.DocumentClient({region: 'us-west-2'});

        console.log(gameId);
        const params = {
            TableName: 'hg_games',
            ScanIndexForward: false,
            KeyConditionExpression: '#game_id = :game_id',
            ExpressionAttributeNames: {
                '#game_id': 'game_id',
            },
            ExpressionAttributeValues: {
                ':game_id': gameId
            }
        };

        readClient.query(params, (err, results) => {
            if (err) {
                reject(err.toString());
            } else {
                if (results.Items.length) {
                    console.log(results.Items);
                    console.log('about to map');
                    const mapped = mapGame(results.Items[0]);
                    console.log('mapped');
                    console.log(mapped);
                    resolve(mapped);//Game(results.Items[0]));
                } else {
                    reject('No results');
                }
            }
        });
    }
});

const mapGame = (dynamoRecord) => {
    return {
        name: dynamoRecord.game_name && dynamoRecord.game_name.S || dynamoRecord.game_name,
        created: dynamoRecord.created && dynamoRecord.created.N || dynamoRecord.created,
        author: dynamoRecord.developer_id && dynamoRecord.developer_id.S || dynamoRecord.developer_id || dynamoRecord.author,
        id: dynamoRecord.game_id && dynamoRecord.game_id.S || dynamoRecord.game_id,
        version: dynamoRecord.version && dynamoRecord.version.N || dynamoRecord.version,
        description: dynamoRecord.description && dynamoRecord.description.S || dynamoRecord.description,
        latest_approved_version: dynamoRecord.latest_approved_version && dynamoRecord.latest_approved_version.N || dynamoRecord.latest_approved_version
    };
};

const updateGame = (developerId, gameId, gameName, description, newVersion) => new Promise((resolve, reject) => {

    if (description) {
        const ddb = new aws.DynamoDB({region: 'us-west-2'});

        const nowString = '' + Date.now();
        const descriptionString = '' + description;
        const params = {
            RequestItems: {
                'hg_games': [
                    {
                        PutRequest: {
                            Item: {
                                'game_composite': {S: `latest:${gameId}`},
                                'game_id': {S: gameId},
                                'developer_id': {S: developerId},
                                'game_name': {S: gameName},
                                'created': {N: nowString},
                                'version': {N: newVersion},
                                'updated': {N: nowString},
                                'description': {S: descriptionString}
                            }
                        }
                    },
                    {
                        PutRequest: {
                            Item: {
                                'game_composite': {S: `${gameId}:${newVersion}`},
                                'game_id': {S: gameId},
                                'developer_id': {S: developer},
                                'game_name': {S: gameName},
                                'created': {N: nowString},
                                'version': {N: newVersion},
                                'updated': {N: nowString},
                                'description': {S: descriptionString}
                            }
                        }
                    }
                ]
            }
        };
        
        const client = new aws.DynamoDB({region: 'us-west-2'});
        client.batchWriteItem(params, (err, putResult) => {
 

            //        const params = {
            //            TableName: 'hg_games',
            //            Item: {
            //                'game_id': {S: gameId},
            //                'developer_id': {S: developerId},
            //                'game_name': {S: gameName},
            //                'created': {N: nowString},
            //                'version': {N: newVersion},
            //                'updated': {N: nowString},
            //                'description': {S: descriptionString}
            //            }
            //        };
            //
            //  console.log('params');
            //      console.log(params);
            //        
            //        ddb.putItem(params, (err, putResult) => {
            if (!err) {
                resolve(mapGame(params.Item));
            } else {
                console.log(err);
                reject();
            }
        });
    } else {
        resolve();
    }

});

const listAssets = (developerId) => new Promise((resolve, reject) => {
    console.log('dddd ' + developerId);
    const client = new aws.DynamoDB({region: 'us-west-2'});

    const params = {
        TableName: 'homegames_assets',
        ScanIndexForward: false,
        KeyConditionExpression: '#developer_id = :developer_id',
        ExpressionAttributeNames: {
            '#developer_id': 'developer_id'
        },
        ExpressionAttributeValues: {
            ':developer_id': {
                S: developerId
            }
        }
    };

    client.query(params, (err, results) => {
        if (!err) {
            console.log('got assets');
            console.log(results);
            const res = results.Items.map(i => {
                return {
                    'developerId': i.developer_id.S,
                    'size': Number(i.size.N),
                    'assetId': i.asset_id.S,
                    'created': Number(i.created_at.N),
                    'status': i['status'].S,
                    'type': JSON.parse(i['metadata'].S)['Content-Type'],
                    'name': i.name && i.name.S || 'No name available'
                };
            });
            resolve(res);
            //            resolve();
        } else {
            console.log(err);
            //          reject(err);
        }
    });

});

const tagGame = (tagId, gameId, userId) => new Promise((resolve, reject) => {
    if (tagId.length == 0 || tagId.length  > 20) {
        reject('tag must be between 1-20 characters');
    }
    const client = new aws.DynamoDB({region: 'us-west-2'});
    const params = {
        TableName: 'hg_tags',
        Item: {
            'user_id': {S: userId},
            'tag': {S: `${userId}:${tagId}`},
            'tag_id': {S: tagId.split(' ')[0]},
            'game_id': {S: gameId},
            'created': {N: `${Date.now()}`}
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


const createRecord = (developerId, assetId, size, name, metadata) => new Promise((resolve, reject) => {
    const client = new aws.DynamoDB({region: 'us-west-2'});
    console.log('creating with');
    console.log(developerId);
    const params = {
        TableName: 'homegames_assets',
        Item: {
            'developer_id': {S: developerId},
            'asset_id': {S: assetId},
            'created': {N: '' + Date.now()},
            'metadata': {S: JSON.stringify(metadata)},
            'status': {S: 'created'},
            'size': {N: '' + size},
            'name': {S: name}
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
        'Authorization': 'Basic ' + Buffer.from(`${GITHUB_USER}:${GITHUB_KEY}`).toString('base64')
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

const createCode = (email, gameId, commit, version) => new Promise((resolve, reject) => {

    const code = getHash(uuidv4());

    const client = new aws.DynamoDB({region: 'us-west-2'});

    const params = {
        TableName: 'hg_requests',
        Item: {
            'code': {S: code},
            'game_id': {S: gameId},
            'commit': {S: commit },
            'status': {S: 'created'},
            'version': {N: version}
        }
    };

    client.putItem(params, (err, putResult) => {
        if (err) {
            reject();
        } else {
            resolve(code);
        }
    });
 
});

const emailOwner = (owner, code, commit) => new Promise((resolve, reject) => {
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
                        Data: `<html><body><a href="https://landlord.homegames.io/confirm?code=${code}&commit=${commit}">here</a> to confirm this submission</body></html>`
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

const doSearch = (searchQuery) => new Promise((resolve, reject) => {
    const data = JSON.stringify({
        query: {
            multi_match: {
                fields: ['game_name', 'author', 'description'],
                query: searchQuery,
                fuzziness: 'AUTO'
            }
            //    query_string: {
            //      {
            //          query: searchQuery
            //      }
            //    },
            //            match: {
            //      game_name: {
            //                  query: searchQuery,
            //        fuzziness: "AUTO"
            //      },
            //      author: {
            //                  query: searchQuery,
            //        fuzziness: "AUTO"
            //      },
            //          description: {
            //                  query: searchQuery,
            //        fuzziness: "AUTO"
            //      } 
            //            },
            //            exists: {
            //              field: 'latest_approved_version'
            //      }
        }
    });

    const options = {
        hostname: ELASTIC_SEARCH_HOST,
        port: 443,
        path: '/lambda-index/_search',
        method: 'GET',
        headers: {
            'Content-Type': 'application/json',
            'Content-Length': data.length
        }
    };

    const req = https.request(options, _res => {
        _res.on('data', d => {
            const buf = JSON.parse(d);
            if (buf.hits && buf.hits.total.value > 0) {
                const results = buf.hits.hits.map(hit => mapGame(hit._source));
                resolve(results);
            } else {
                resolve();
            }
    
        });
    });

    req.write(data);

    req.end();
});

const DEFAULT_GAME_ORDER = {'game_name': {order: 'asc'}};

const listGames = (limit = 10, offset = 0, sort = DEFAULT_GAME_ORDER, query = null, tags = []) => new Promise((resolve, reject) => {

    const _data = {
        from: offset,
        size: limit
    };

    console.log("I GOT TAGS");
    console.log(tags);

    let data, options;

    if (tags.length > 0) {
        _data.query = {
        terms: {
            tag_id: tags
        }
        };

        data = JSON.stringify(_data);

        options = {
            hostname: ELASTIC_SEARCH_HOST,
            port: 443,
            path: '/tag-index/_search',
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
                'Content-Length': data.length
            }
        };

        // this is terrible
        
    let gameIds = [];

        const req = https.request(options, _res => {
            let _buf = '';
            _res.on('data', d => {
                _buf += d;
            });
    
            _res.on('end', () => {
                const buf = JSON.parse(_buf);
                console.log('tag response');
                console.log(buf);
        if (buf.hits && buf.hits.hits) {
                gameIds = buf.hits.hits.map(hit => hit._source);
        }

        const gameQuery = {
            query: {
                bool: {
                    must: [
                        {
                            exists: {
                                field: 'latest_approved_version'
                            },
                        },
                        {
                            terms: {
                                game_id: gameIds
                            }
                        },
                    ]
                }
            },
            sort
        };

        const gameReq = JSON.stringify(gameQuery);

            const gameOptions = {
                hostname: ELASTIC_SEARCH_HOST,
                port: 443,
                path: '/lambda-index/_search',
                method: 'GET',
                headers: {
                    'Content-Type': 'application/json',
                    'Content-Length': gameReq.length
                }
            };
    
            const req2 = https.request(gameOptions, _res2 => {
                let _buf2 = '';
                _res2.on('data', d2 => {
                    _buf2 += d2;
                });
        
                _res2.on('end', () => {
                    const buf2 = JSON.parse(_buf2);
                    console.log('games from tags giigig');
                    console.log(buf2);
                    let gameList = [];
                    if (buf2.hits && buf2.hits.total.value > 0) {
                        console.log('I know I have games');
                        console.log(buf2.hits.hits);
                        gameList = buf2.hits.hits.map(hit => mapGame(hit._source));
                    }
        
                    resolve(gameList);
            });
        });

        req2.write(gameReq);
        req2.end();

            });
    
        });
    
        req.write(data);
    
        req.end();

    } else {
        _data.query = {
            exists: {
                field: 'latest_approved_version'
            }
        };
        
    _data.sort = sort;
 
        if (query) {
            _data.query.query_string = query;
        }

        data = JSON.stringify(_data);
    
        options = {
            hostname: ELASTIC_SEARCH_HOST,
            port: 443,
            path: '/lambda-index/_search',
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
                'Content-Length': data.length
            }
        };

        const req = https.request(options, _res => {
            let _buf = '';
            _res.on('data', d => {
                _buf += d;
            });
    
            _res.on('end', () => {
                const buf = JSON.parse(_buf);
                console.log('giigig');
                console.log(buf);
                let gameList = [];
                if (buf.hits && buf.hits.total.value > 0) {
                    console.log('I know I have games');
                    console.log(buf.hits.hits);
                    gameList = buf.hits.hits.map(hit => mapGame(hit._source));
                }
    
                resolve(gameList);
            });
    
        });
    
        req.write(data);
    
        req.end();
    }
});

const getGameInstance = (owner, repo, commit) => new Promise((resolve, reject) => {

    getCommit(owner, repo, commit).then(_res => {

        getBuild(owner, repo, commit).then((dir) => {
            const cmd = ['--prefix', dir.path, 'install'];
            const { exec } = require('child_process');
            exec('npm --prefix ' + dir.path + ' install', (err, stdout, stderr) => {
                const _game = require(dir.path);
                resolve(_game);
            });
            // todo: this
            //const sourceNodeModules = 'INSERTHERE';
            //            fs.symlink(sourceNodeModules, dir + '/node_modules', 'dir', (err) => {
            //                if (!err) {
            //                    const _game = require(dir);
            //                    resolve(_game);
            //                } else {
            //                    reject(err);
            //                }
            //            });
        });
    
    });

});

const server = http.createServer((req, res) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Headers', '*');

    if (req.method === 'GET') {
        const gameDetailRegex = new RegExp('/games/(\\S*)');
        if (req.url == '/assets') {
            const username = req.headers['hg-username'];
            const token = req.headers['hg-token'];

            if (!username || !token) {
                res.end('no');
            } else {
                verifyAccessToken(username, token).then(() => {
                    listAssets(username).then(assets => {
                        res.writeHead(200, {
                            'Content-Type': 'application/json'
                        });
 
                        res.end(JSON.stringify({ assets }));
                    });
                }).catch((err) => {
                    console.log(err);
                    res.end('error');
                });
            }
        } else if (req.url.startsWith('/confirm')) {
            const queryObject = url.parse(req.url,true).query;

            const code = queryObject.code;
            const commit = queryObject.commit;

            const ddb = new aws.DynamoDB({region: 'us-west-2'});

            const params = {
                TableName: 'hg_requests',
                Key: {
                    'code': {S: code},
                    'commit': {S: commit}
                }
            };

            ddb.getItem(params, (err, data) => {
                if (data) {
                    const gameId = data.Item.game_id.S;
                    const version = data.Item.version.N;
                    console.log('need to approve ' + gameId);
                    const gameVersion = ddb.getItem({
                        TableName: 'hg_game_versions',
                        Key: {
                            'game_id': {S: gameId},
                            'version': {N: '' + version}
                        }
                    }, (err, data) => {
                        console.log('got data');
                        console.log(data);
                        
                        const updateParams = {
                            TableName: 'hg_game_versions',
                            Key: {
                                'game_id': {S: gameId},
                                'version': {N: '' + version}
                            },
                            AttributeUpdates: {
                                'status': {
                                    Action: 'PUT', 
                                    Value: {
                                        S: 'approved' 
                                    }
                                },
                                'approved_at': {
                                    Action: 'PUT',
                                    Value: {
                                        N: '' + Date.now()
                                    }
                                }
                            }
                        };

                        const reqUpdateParams = {
                            TableName: 'hg_requests',
                            Key: {
                                'code': {S: code},
                                'commit': {S: commit}
                            },
                            AttributeUpdates: {
                                'status': {
                                    Action: 'PUT',
                                    Value: {
                                        S: 'approved'
                                    }
                                },
                                'approved_at': {
                                    Action: 'PUT',
                                    Value: {
                                        N: '' + Date.now()
                                    }
                                }
                            }
                        };

                        ddb.updateItem(updateParams, (err, putResult) => {
                            if (!err) {
                                ddb.updateItem(reqUpdateParams, (err, putResult2) => {
                                    if (!err) {
                                        res.end('nice');
                                    } else {
                                        res.end('no');
                                    }
                                });
                            } else {
                                res.end('no 2');
                            }
                        });
                    });
                }
            });

        } else if (req.url.match(gameDetailRegex)) {
            const gameId = gameDetailRegex.exec(req.url)[1].split('?')[0];

            const client = new aws.DynamoDB.DocumentClient({region: 'us-west-2'});

            const params = {
                TableName: 'hg_game_versions',
                ScanIndexForward: false,
                KeyConditionExpression: '#game_id = :game_id',
                ExpressionAttributeNames: {
                    '#game_id': 'game_id'
                },
                ExpressionAttributeValues: {
                    ':game_id': gameId
                }
            };

            client.query(params, (err, data) => {
                const results = data.Items.map(i => {
                    return {
                        version: i.version,
                        created: new Date(i.created),
                        commit: i.commit,
                        'status': i.status,
                        'location': i.location
                    };
                });

                const getTags = (__gameId) => new Promise((__resolve, __reject) => {
                    const __data = JSON.stringify({
                        query: {
                            term: {
                                'game_id': {
                                    value: __gameId
                                }
                            }
                        },
                        aggs: {
                            tags: {
                                terms: {
                                    field: 'tag_id'
                                }
                            }
                        }
                
                    });
            
                    const __options = {
                        hostname: ELASTIC_SEARCH_HOST,
                        port: 443,
                        path: '/tag-index/_search?size=0',
                        method: 'GET',
                        headers: {
                            'Content-Type': 'application/json',
                            'Content-Length': __data.length
                        }
                    };
            
                    const __req = https.request(__options, __res => {
                        __res.on('data', __d => {
                            const __buf = JSON.parse(__d);
                            console.log(__buf);
                            if (__buf.aggregations && __buf.aggregations.tags.buckets.length > 0) {
                                const __results = __buf.aggregations.tags.buckets.map(item => item.key);//mapGame(hit._source));
                                __resolve(__results);
                            } else {
                                __resolve([]);
                            }
                
                        });
                    });
            
                    __req.write(__data);
            
                    __req.end();
                });


        

                getTags(gameId).then(tags => {

                    console.log('GGGG TAGS');
                    console.log(tags);
                    res.writeHead(200, {
                        'Content-Type': 'application/json'
                    });
                    res.end(JSON.stringify({ tags, versions: results }));
                });

            });
        } else if (req.url.startsWith('/games')) {
            const client = new aws.DynamoDB.DocumentClient({region: 'us-west-2'});
            const queryObject = url.parse(req.url,true).query;

            const searchQuery = queryObject.query;
            const tags = queryObject.tags;

            const requester = req.headers['hg-username'];
                            
            if (queryObject.author) {
                console.log('want games for just author');
                const queryParams = {
                    TableName: 'hg_games',
                    ScanIndexForward: false,
                    KeyConditionExpression: '#developer_id = :developer_id and begins_with(#game_composite, :latest)',
                    ExpressionAttributeNames: {
                        '#developer_id': 'developer_id',
                        '#game_composite': 'game_composite'
                    },
                    ExpressionAttributeValues: {
                        ':developer_id': queryObject.author,
                        ':latest': 'latest'
                    }
                };

                client.query(queryParams, (err, data) => {
                    console.log(err);
                    if (!err) {
                        const gameList = data.Items.map(mapGame);
                        res.writeHead(200, {
                            'Content-Type': 'application/json'
                        });

                        res.end(JSON.stringify({games: gameList}));
                    } else {
                        res.end({error: err});
                    }
                });
            } else if (searchQuery) {
                doSearch(searchQuery).then(d => {
                    res.end(JSON.stringify({games: d}));
                });
                //              listGames(10, 0, DEFAULT_GAME_ORDER, searchQuery).then(d => {
                //                      res.end(JSON.stringify({games: d}));
                //          });
            } else {
                listGames(10, 0, DEFAULT_GAME_ORDER, searchQuery, tags.split(',')).then(d => {
                    console.log('GAMES');
                    console.log(d);
                    res.end(JSON.stringify({games: d}));
                });


                //                const sort = queryObject.sort || 'name';
                //                const order = queryObject.order || 'asc';
                //
                //                const indexMap = {
                //                    'name': 'game_name_sort_index',
                //                    'created': 'created_at_sort_index',
                //                    'updated': 'updated_at_sort_index'
                //                };
                //
                //                let params = {};
                //
                //                if (!requester) {
                //                    params = {
                //                        TableName: 'hg_games',
                //                        ScanIndexForward: order === 'asc',
                //                        IndexName: indexMap[sort] || indexMap['name'],
                //                        KeyConditionExpression: '#dummy = :dummy',
                //                        ExpressionAttributeNames: {
                //                            '#dummy': 'dummy'
                //                        },
                //                        ExpressionAttributeValues: {
                //                            ':dummy': 'dummy'
                //                        }
                //                    };
                //                } else {
                //                    params = {
                //                        TableName: 'hg_games',
                //                        ScanIndexForward: order === 'asc',
                //                        KeyConditionExpression: '#devId= :devId',
                //                        ExpressionAttributeNames: {
                //                            '#devId': 'developer_id'
                //                        },
                //                        ExpressionAttributeValues: {
                //                            ':devId': requester 
                //                        }
                //                    };
                //                }
                //
                //                client.query(params, (err, data) => {
                //                    if (err) {
                //                        res.end(err.toString());
                //                    } else {
                //                        res.writeHead(200, {
                //                            'Content-Type': 'application/json'
                //                        });
                //
                //                        res.end(JSON.stringify({
                //                            games: data.Items.map(g => {
                //                                return {
                //                                    created: g.created_at,
                //                                    updated: g.updated_at,
                //                                    id: g.game_id,
                //                                    submitted_by: g.developer_id,
                //                                    name: g.game_name,
                //                                    description: g.description,
                //                                    thumbnail: g.thumbnail
                //                                }
                //                            })
                //                        }));
                //                    }
                //      });
            }
        } else {
            res.end('ok');
        } 
    } else if (req.method === 'POST') {
        const gamePublishRegex = new RegExp('/games/(\\S*)/publish');
        const gameUpdateRegex = new RegExp('/games/(\\S*)/update');
        if (req.url.match(gameUpdateRegex)) {
            const username = req.headers['hg-username'];
            const token = req.headers['hg-token'];

            if (!username || !token) {
                res.end('no');
            } else {
                verifyAccessToken(username, token).then(() => {
                    const form = new multiparty.Form();
 
                    getReqBody(req, (_data) => {
                        const data = JSON.parse(_data);
                        const client = new aws.DynamoDB({region: 'us-west-2'});
                        const readClient = new aws.DynamoDB.DocumentClient({region: 'us-west-2'});

                        const _gameUpdateRegex = new RegExp('/games/(\\S*)/update');
                        const gameId = _gameUpdateRegex.exec(req.url)[1];
                        console.log('developer ' + username + ' wants to update ' + gameId);
                        console.log(data);
                        const changed = data.description || data.thumbnail;

                        if (changed) {
                            getGame(gameId).then(game => {
                                if (username != game.author) {
                                    res.writeHead(400, {'Content-Type': 'text/plain'});
                                    res.end('You cannot modify a game that you didnt create');
                                } else {
                                    const newVersion = '' + (Number(game.version) + 1);
                                    if (data.description != game.description) {
                                        updateGame(username, gameId, game.name, data.description, newVersion).then((_game) => {
                                            // sigh. 
                                            setTimeout(() => {
                                                res.writeHead(200, {'Content-Type': 'application/json'});
                                                res.end(JSON.stringify(_game));
                                            }, 250);
                                        });
                                    } else {
                                        res.end('hmmm');
                                    }
                                }
                            }).catch(err => {
                                res.end(err.toString());
                            });
                        } else {
                            res.writeHead(400, {'Content-Type': 'text/plain'});
                            res.end('No valid changes');
                        }
                    });
                });
            }
        } else if (req.url == '/tags') {
            const username = req.headers['hg-username'];
            const token = req.headers['hg-token'];

            if (!username || !token) {
                res.end('no');
            } else {
                verifyAccessToken(username, token).then(() => {
 
                    getReqBody(req, (_data) => {
                        const data = JSON.parse(_data);
                        if (!data.game_id || !data.tag) {
                            res.end('request requires game id & tag');
                        } else {
                            console.log("create a tag");
                            console.log(data);
                            tagGame(data.tag, data.game_id, username).then(() => {
                                res.writeHead(200, {'Content-Type': 'application/json'});
                                res.end('nice');
                            }).catch(err => {
                                console.log(err);
                                res.end('something went wrong');
                            });
                        }
                    });
                });
            }
        } else if (req.url.match(gamePublishRegex)) {
            const username = req.headers['hg-username'];
            const token = req.headers['hg-token'];

            if (!username || !token) {
                res.end('no');
            } else {
                verifyAccessToken(username, token).then(() => {
                    const form = new multiparty.Form();
 
                    getReqBody(req, (_data) => {
                        const data = JSON.parse(_data);
                        console.log('want to publish game');
                        const client = new aws.DynamoDB({region: 'us-west-2'});
                        const readClient = new aws.DynamoDB.DocumentClient({region: 'us-west-2'});

                        const _gamePublishRegex = new RegExp('/games/(\\S*)/publish');
                        const gameId = _gamePublishRegex.exec(req.url)[1];
                        const params = {
                            TableName: 'hg_game_versions',
                            KeyConditionExpression: '#gameId = :gameId',
                            Limit: 1,
                            ScanIndexForward: false,
                            ExpressionAttributeNames: {
                                '#gameId': 'game_id'
                            },
                            ExpressionAttributeValues: {
                                ':gameId': gameId
                            }
                        };

                        readClient.query(params, (err, result) => {
                            if (err) {
                                res.end(err);
                            } else {

                                let busted = false;
                                let newVersion = '1';

                                if (result.Items.length) {
                                    console.log(result);
                                    if (result.Items[0].commit == data.commit) {
                                        res.end('already built ' + data.commit);
                                        busted = true;
                                    } else {
                                        newVersion = '' + (result.Items[0].version + 1);
                                    }
                                } 

                                // hack
                                if (!busted) {

                                    getBuild(data.owner, data.repo, data.commit).then((dir) => {
                                        
                                        const s3 = new aws.S3({region: 'us-west-2'});
                                        fs.readFile(dir.zipPath, (err, buf) => {
                                            console.log(err);

                                            const params = {
                                                Body: buf,
                                                ACL: 'public-read',
                                                Bucket: 'hg-games',
                                                Key: `${gameId}/${data.commit}.zip`
                                            };

                                            s3.putObject(params, (err, s3Data) => {
                                                console.log('data');
                                                const updateGameLatest = () => new Promise((resolve, reject) => {
                                
                                                    getGame(gameId).then((_game) => { 
                                                        console.log('GOT GAMEEEEE');
                                                        console.log(_game);
                                                        const _updateParams = {
                                                            TableName: 'hg_games',
                                                            Key: {
                                                                'game_id': {S: gameId},
                                                                'version': {S: _game.version}
                                                            },
                                                            AttributeUpdates: {
                                                                'status': {
                                                                    Action: 'PUT',
                                                                    Value: {
                                                                        S: 'approved'
                                                                    }
                                                                },
                                                                'approved_at': {
                                                                    Action: 'PUT',
                                                                    Value: {
                                                                        N: '' + Date.now()
                                                                    }
                                                                }
                                                            }
                                                        };
            
            
                                                        ddb.updateItem(updateParams, (err, putResult) => {
                                                            if (!err) {
                                                                ddb.updateItem(reqUpdateParams, (err, putResult2) => {
                                                                    if (!err) {

                                                                        const _location = `https://hg-games.s3-us-west-2.amazonaws.com/${gameId}/${data.commit}.zip`;
            
                                                                        const params = {
                                                                            TableName: 'hg_game_versions',
                                                                            Item: {
                                                                                'game_id': {S: gameId},
                                                                                'version': {N: newVersion},
                                                                                'created': {N: '' + Date.now()},
                                                                                'submitted_by': {S: 'todo'},
                                                                                'location': {S: _location},
                                                                                'commit': {S: data.commit},
                                                                                'status': {S: 'created'}
                                                                            }
                                                                        };
        
                                                                        client.putItem(params, (err, putResult) => {
                                                                            console.log(err);
                                                                            console.log(putResult);
                                                                            getGameInstance(data.owner, data.repo, data.commit).then(game => {
                                                                                testGame(game).then(() => {
                                                                                    console.log('emailing ' + data.owner);
                                                                                    getOwnerEmail(data.owner).then(_email => {
                                                                                        console.log(_email);
                                                                                        createCode(_email, gameId, data.commit, newVersion).then((code) => {
                                                                                            console.log('created code');
                                                                                            emailOwner(data.owner, code, data.commit).then(() => {
                                                                                                res.end('emailed owner!');
                                                                                            }).catch(err => {
                                                                                                console.error(err);
                                                                                                res.end('error');
                                                                                            });
                                                                                        });
                                                                                    });
                                                                                });
                                                                            });
                                                                        }); 
        
                                                                    } else {
                                                                        console.log(err);
                                                                        res.end('hmmmmmm');
                                                                    }
                                                                });
                                                            } else {
                                                                console.log(err);
                                                                res.end('hmmm');
                                                            }
                                                        });
                                                    });

                                                });
                                            });
                                        });
                                    });
                                }
                            }
                        });
                    });
                });
            }
        } else if (req.url === '/asset') {
            const username = req.headers['hg-username'];
            const token = req.headers['hg-token'];

            if (!username || !token) {
                res.end('no');
            } else {
                verifyAccessToken(username, token).then(() => {
                    const form = new multiparty.Form();
                    form.parse(req, (err, fields, files) => {
                        if (!files) {
                            res.end('no');
                        } else {
                            const fileValues = Object.values(files);
                            console.log(files);
    
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
    
                                    const username = req.headers['hg-username'];
                                    console.log('hello');
                                    console.log(username);
                                    // todo: auth
                                    createRecord(username, assetId, f.size, f.originalFilename, {
                                        'Content-Type': f.headers['content-type']
                                    }).then(() => {
    
                                        const childSession = fork(path.join(__dirname, 'upload.js'), 
                                            [
                                                `--path=${f.path}`, 
                                                `--developer=${username}`, 
                                                `--id=${assetId}`, 
                                                `--name=${f.originalFilename}`, 
                                                `size=${f.size}`, 
                                                `type=${f.headers['content-type']}`
                                            ]
                                        );
                                        res.writeHead(200, {'Content-Type': 'application/json'});
                                        res.end(JSON.stringify({
                                            assetId
                                        }));
    
                                    });
                                }
                            });
                        }
    
                    });
                }).catch((err) => {
                    console.log(err);
                    console.log('wat');
                    res.end('error');
                });
            }
        } else if (req.url === '/games') {
            const username = req.headers['hg-username'];
            const token = req.headers['hg-token'];

            if (!username || !token) {
                res.end('no');
            } else {
                verifyAccessToken(username, token).then(() => {
                    getReqBody(req, (_data) => {
                        console.log('sdd');
                        const data = JSON.parse(_data);
        
                        const readClient = new aws.DynamoDB.DocumentClient({region: 'us-west-2'});
        
                        const params = {
                            TableName: 'hg_games',
                            IndexName: 'dev_game_name_index',
                            KeyConditionExpression: '#devId = :devId and #gameName = :gameName',
                            ExpressionAttributeNames: {
                                '#devId': 'developer_id',
                                '#gameName': 'game_name'
                            },
                            ExpressionAttributeValues: {
                                ':devId': username,
                                ':gameName': data.game_name
                            }
                        };
        
                        readClient.query(params, (err, result) => {
                            if (err) {
                                res.end(err.toString());
                            } else {
                                if (result.Items.length) {
                                    res.writeHead(400, {'Content-Type': 'text/plain'});
                                    res.end('that already exists');
                                } else {
                                    const gameId = generateGameId();
                                    const nowString = '' + Date.now();
                                    const descriptionString = ('' + data.description) || 'No description provided';
                                    console.log('wat');
                                    const params = {
                                        RequestItems: {
                                            'hg_games': [
                                                {
                                                    PutRequest: {
                                                        Item: {
                                                            'game_composite': {S: `latest:${gameId}`},
                                                            'game_id': {S: gameId},
                                                            'developer_id': {S: username},
                                                            'game_name': {S: data.game_name},
                                                            'created': {N: nowString},
                                                            'version': {N: '1'},
                                                            'updated': {N: nowString},
                                                            'description': {S: descriptionString}
                                                        }
                                                    }
                                                },
                                                {
                                                    PutRequest: {
                                                        Item: {
                                                            'game_composite': {S: `${gameId}:1`},
                                                            'game_id': {S: gameId},
                                                            'developer_id': {S: username},
                                                            'game_name': {S: data.game_name},
                                                            'created': {N: nowString},
                                                            'version': {N: '1'},
                                                            'updated': {N: nowString},
                                                            'description': {S: descriptionString}
                                                        }
                                                    }
                                                }
                                            ]
                                        }
                                    };
        
                                    const client = new aws.DynamoDB({region: 'us-west-2'});
                                    client.batchWriteItem(params, (err, putResult) => {
        
                                        console.log(err);
                                        console.log(putResult);

                                        if (!err) {
                                            res.writeHead(200, {
                                                'Content-Type': 'application/json'
                                            });
                                            res.end(JSON.stringify(mapGame(params.RequestItems.hg_games[0].PutRequest.Item)));
                                        } else {
                                            console.log(err);
                                            res.end('error');
                                        }
                                    });
                                }
                            }
                        });
                    });
                }).catch(err => {
                    console.log(err);
                    res.end((err.errorMessage || err).toString());
                });
            }
        }
    } else {
        res.end('not found');
    }
});
    
server.listen(80);
