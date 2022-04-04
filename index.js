const http = require('http');
const https = require('https');
const unzipper = require('unzipper');
const url = require('url');
const archiver = require('archiver');
const fs = require('fs');
const aws = require('aws-sdk');
const querystring = require('querystring');
const process = require('process');
const {
    v4: uuidv4
} = require('uuid');
const crypto = require('crypto');
const {
    parse
} = require('querystring');
const multiparty = require('multiparty');
const {
    fork
} = require('child_process');
const path = require('path');
const {
    verifyAccessToken
} = require('homegames-common');

const SourceType = {
    GITHUB: 'GITHUB'
};

const QUEUE_URL = process.env.SQS_QUEUE_URL;

const poolData = {
    UserPoolId: process.env.COGNITO_USER_POOL_ID,
    ClientId: process.env.COGNITO_CLIENT_ID
};

const getCognitoUser = (username) => new Promise((resolve, reject) => {
    const params = {
        UserPoolId: poolData.UserPoolId,
        Username: username
    };

    const provider = new aws.CognitoIdentityServiceProvider({region: 'us-west-2'});

    provider.adminGetUser(params, (err, data) => {
        if (err) {
            console.error(err);
            reject('failed to get user');
        } else {
            const isAdminAttribute = data.UserAttributes && data.UserAttributes.filter(a => a.Name === 'custom:isAdmin').length > 0 ? data.UserAttributes.filter(a => a.Name === 'custom:isAdmin')[0] : null;
            resolve({
                created: data.UserCreateDate,
                isAdmin: isAdminAttribute && isAdminAttribute.Value === 'true' ? true : false
            });
        }
    });
});

const uploadThumbnail = (username, gameId, thumbnail) => new Promise((resolve, reject) => {
    console.log('need to upload to s3');
    console.log(thumbnail);
    console.log(thumbnail.headers);
    const assetId = generateId();

    const childSession = fork(path.join(__dirname, 'upload.js'),
        [
            `--path=${thumbnail.path}`,
            `--developer=${username}`,
            `--id=${assetId}`,
            `--name=${thumbnail.originalFilename}`,
            `--size=${thumbnail.size}`,
            `--type=${thumbnail.headers['content-type']}`
        ]
    );

    resolve('https://assets.homegames.io/' + assetId);
});

const getLatestGameVersion = (gameId) => new Promise((resolve, reject) => {
    const readClient = new aws.DynamoDB.DocumentClient({
        region: 'us-west-2'
    });

    console.log(gameId);
    const params = {
        TableName: 'game_versions',
        ScanIndexForward: false,
        Limit: 1,
        KeyConditionExpression: '#game_id = :game_id',
        ExpressionAttributeNames: {
            '#game_id': 'game_id',
        },
        ExpressionAttributeValues: {
            ':game_id': gameId
        }
    };

    readClient.query(params, (err, results) => {
        console.log('getLatestGameVerison results and err');
        console.log(err);
        console.log(results);

        if (err) {
            reject(err.toString());
        } else {
            if (results.Items.length) {
                resolve(Number(results.Items[0].version));
            } else {
                resolve(null);

            }
        }
    });
 
});

const publishGameVersion = (publishRequest) => new Promise((resolve, reject) => {
    console.log('you want to publish this game');
    console.log(publishRequest);
    const gameId = publishRequest.game_id;
    const requestId = publishRequest.request_id;

    const s3Url = getS3Url(gameId, requestId);

    // verify code is publicly available
    https.get(s3Url, (res) => {
        if (res.statusCode != 200) {
            console.log('bad status code');
            reject();
        } else {
            getLatestGameVersion(gameId).then(currentVersion => {
                const client = new aws.DynamoDB({
                    region: 'us-west-2'
                });

                const newVersion = currentVersion ? currentVersion + 1 : 1;
                getGame(gameId, publishRequest.requester).then(gameData => {
	
                    const params = {
                        TableName: 'game_versions',
                        Item: {
                            'version': {
                                N: '' + newVersion
                            },
                            'commit_hash': {
                                S: publishRequest.commit_hash
                            },
                            'description': {
                                S: publishRequest.notes || 'No description available'
                            },
                            'location': {
                                S: s3Url
                            },
                            'published_at': {
                                N: '' + Date.now()
                            },
                            'published_by': {
                                S: publishRequest.requester
                            },
                            'request_id': {
                                S: requestId
                            },
                            'game_id': {
                                S: gameId
                            },
                            'version_id': {
                                S: generateId()
                            }
                        }
                    };
		
                    client.putItem(params, (err, putResult) => {
                        if (!err) {
                            console.log('published new game version of game id ' + gameId);
                            resolve();
                        } else {
                            console.log('failed to publish version');
                            console.log(err);
                            reject(err);
                        }
                    });
                });
            });
        }
    });
});

const requestIdFromCode = (code) => new Promise((resolve, reject) => {
    const ddb = new aws.DynamoDB({
        region: 'us-west-2'
    });

    const codeParams = {
        TableName: 'verification_requests',
        IndexName: 'code-index',
        Key: {
            'code': {
                S: code
            }
        }
    };

    ddb.getItem(codeParams, (err, data) => {
        if (err) {
            console.log(err);
            reject();
        } else {
            const requestId = data.Item.publish_request_id.S;
            resolve(request);
        }
    });

});

const getPublishRequestFromCode = (code) => new Promise((resolve, reject) => {
    requestIdFromCode(code).then(requestId => {
        const ddb = new aws.DynamoDB({
            region: 'us-west-2'
        });

        const params = {
            TableName: 'publish_requests',
            IndexName: 'request_id-index',
            Key: {
                'request_id': {
                    S: requestId
                }
            }
        };

        ddb.getItem(params, (err, data) => {
            if (err) {
                console.log(err);
                reject();
            } else {
                console.log('got request');
                console.log(data.Item);
                resolve();
            }
        });
    });
});

const verifyCode = (code, requestId) => new Promise((resolve, reject) => {
    console.log(`verifying code ${code} with request ${requestId}`);
    const ddb = new aws.DynamoDB({
        region: 'us-west-2'
    });

    const params = {
        TableName: 'verification_requests',
        Key: {
            'publish_request_id': {
                S: requestId
            }
        }
    };

    ddb.getItem(params, (err, data) => {
        if (err) {
            console.log(err);
            reject();
        } else {
            console.log('verification request');
            console.log(data.Item);
            const _requestCode = data.Item.code.S;
            if (code == _requestCode) {
                resolve();
            } else {
                console.log('requested code doesnt match code in record');
                reject();
            }
        }
    });
});

const getPublishRequest = (requestId) => new Promise((resolve, reject) => {
    const readClient = new aws.DynamoDB.DocumentClient({
        region: 'us-west-2'
    });

    const params = {
        TableName: 'publish_requests',
        IndexName: 'request_id-index',
        KeyConditionExpression: '#requestId = :requestId',
        ExpressionAttributeNames: {
            '#requestId': 'request_id'
        },
        ExpressionAttributeValues: {
            ':requestId': requestId
        }
    };

    readClient.query(params, (err, result) => {
        if (err) {
            reject();
        } else {
            if (result.Items.length != 1) {
                console.log('wrong length');
                console.log(result.Items);
                reject();
            } else {
                resolve(result.Items[0]);
            }
        }
    });
 
});


// copied from homedome
const getS3Url = (gameId, requestId) => {
    return `https://hg-games.s3-us-west-2.amazonaws.com/${gameId}/${requestId}/code.zip`;
};

const updatePublishRequestState = (gameId, sourceInfoHash, newStatus) => new Promise((resolve, reject) => {
    const ddb = new aws.DynamoDB({
        region: 'us-west-2'
    });

    const updateParams = {
        TableName: 'publish_requests',
        Key: {
            'game_id': {
                S: gameId
            },
            'source_info_hash': {
                S: sourceInfoHash
            }
        },
        AttributeUpdates: {
            'status': {
                Action: 'PUT',
                Value: {
                    S: newStatus
                }
            }
        }
    };

    ddb.updateItem(updateParams, (err, putResult) => {
        console.log(err);
        if (err) {
            reject();
        } else {
            resolve();
        }
    });
});

const emitEvent = (requestId, eventType, message = null) => new Promise((resolve, reject) => {

    const client = new aws.DynamoDB({
        region: 'us-west-2'
    });

    const params = {
        TableName: 'publish_events',
        Item: {
            'request_id': {
                S: requestId
            },
            'event_date': {
                N: `${Date.now()}`
            },
            'event_type': {
                S: eventType
            }
        }
    };

    if (message != null) {
        params.Item.message = {S: message};
    }

    client.putItem(params, (err, putResult) => {
        if (!err) {
            resolve();
        } else {
            reject(err);
        }
    });
});

// okay that was homedome stuff. TODO: define better relationship between landlord and homedome

const verifyPublishRequest = (code, requestId) => new Promise((resolve, reject) => {
    emitEvent(requestId, 'VERIFICATION_ATTEMPT', 'Attempting to verify publish request from email code').then(() => {
        verifyCode(code, requestId).then(() => {
            console.log('verified code exists');
            getPublishRequest(requestId).then(requestData => {
                const { game_id, source_info_hash } = requestData;
                console.log('checking approval status of ');
                console.log(requestData);
                if (requestData.status == 'APPROVED') {
                    reject('already approved');
                } else {
                    emitEvent(requestId, 'VERIFICATION_SUCCESS').then(() => {
                        updatePublishRequestState(game_id, source_info_hash, 'APPROVED').then(() => {
                            console.log('updated status');
                            resolve(requestData);
                        });
                    });
                }
            });
        }).catch(err => {
            console.log('verify error ' + err);
            emitEvent(requestId, 'VERIFICATION_ERROR', 'Failed verifying code').then(() => {
                reject();
            });
        });
    });
});

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

        const stream = _response.pipe(unzipper.Extract({
            path: dir
        }));

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

const generateId = () => getHash(uuidv4());

const getReqBody = (req, cb) => {
    let _body = '';
    req.on('data', chunk => {
        _body += chunk.toString();
    });

    req.on('end', () => {
        cb && cb(_body);
    });
};

const getGame = (gameId, developerId, version = null) => new Promise((resolve, reject) => {

    const client = new aws.DynamoDB({
        region: 'us-west-2'
    });

    const params = {
        TableName: 'games',
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
});

const updateGame = (developerId, gameId, gameName, description, newVersion) => new Promise((resolve, reject) => {
    console.log('updating game: ' + gameId);

    if (description) {
        const ddb = new aws.DynamoDB({
            region: 'us-west-2'
        });

        const nowString = '' + Date.now();
        const descriptionString = '' + description;
        const params = {
            RequestItems: {
                'hg_games': [{
                    PutRequest: {
                        Item: {
                            'game_composite': {
                                S: `latest:${gameId}`
                            },
                            'game_id': {
                                S: gameId
                            },
                            'developer_id': {
                                S: developerId
                            },
                            'game_name': {
                                S: gameName
                            },
                            'created': {
                                N: nowString
                            },
                            'version': {
                                N: newVersion
                            },
                            'updated': {
                                N: nowString
                            },
                            'description': {
                                S: descriptionString
                            }
                        }
                    }
                },
                {
                    PutRequest: {
                        Item: {
                            'game_composite': {
                                S: `${gameId}:${newVersion}`
                            },
                            'game_id': {
                                S: gameId
                            },
                            'developer_id': {
                                S: developerId
                            },
                            'game_name': {
                                S: gameName
                            },
                            'created': {
                                N: nowString
                            },
                            'version': {
                                N: newVersion
                            },
                            'updated': {
                                N: nowString
                            },
                            'description': {
                                S: descriptionString
                            }
                        }
                    }
                }
                ]
            }
        };

        const client = new aws.DynamoDB({
            region: 'us-west-2'
        });
        client.batchWriteItem(params, (err, putResult) => {
            if (!err) {
                resolve(mapGame(params.RequestItems.hg_games[0].PutRequest.Item));
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
    const client = new aws.DynamoDB({
        region: 'us-west-2'
    });

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
        } else {
            console.log(err);
            reject();
        }
    });

});

const tagGame = (tagId, gameId, userId) => new Promise((resolve, reject) => {
    if (tagId.length == 0 || tagId.length > 20) {
        reject('tag must be between 1-20 characters');
    }
    const client = new aws.DynamoDB({
        region: 'us-west-2'
    });
    const params = {
        TableName: 'hg_tags',
        Item: {
            'user_id': {
                S: userId
            },
            'tag': {
                S: `${userId}:${tagId}`
            },
            'tag_id': {
                S: tagId.split(' ')[0]
            },
            'game_id': {
                S: gameId
            },
            'created': {
                N: `${Date.now()}`
            }
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
    const client = new aws.DynamoDB({
        region: 'us-west-2'
    });
    console.log('creating with');
    console.log(developerId);
    const params = {
        TableName: 'homegames_assets',
        Item: {
            'developer_id': {
                S: developerId
            },
            'asset_id': {
                S: assetId
            },
            'created_at': {
                N: '' + Date.now()
            },
            'metadata': {
                S: JSON.stringify(metadata)
            },
            'status': {
                S: 'created'
            },
            'size': {
                N: '' + size
            },
            'name': {
                S: name
            }
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

    const client = new aws.DynamoDB({
        region: 'us-west-2'
    });

    const params = {
        TableName: 'hg_requests',
        Item: {
            'code': {
                S: code
            },
            'game_id': {
                S: gameId
            },
            'commit': {
                S: commit
            },
            'status': {
                S: 'created'
            },
            'version': {
                N: version
            }
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

const testGame = (game) => new Promise((resolve, reject) => {
    const instance = new game();
    // todo: make this good
    if (instance.getRoot()) {
        resolve();
    } else {
        reject();
    }
});

const DEFAULT_GAME_ORDER = {
    'game_name': {
        order: 'asc'
    }
};

const getPublishRequestEvents = (requestId) => new Promise((resolve, reject) => {
    console.log('you want publish request events for this request! ' + requestId);
    const client = new aws.DynamoDB.DocumentClient({
        region: 'us-west-2'
    });

    const params = {
        TableName: 'publish_events',
        KeyConditionExpression: '#request_id= :request_id',
        ExpressionAttributeNames: {
            '#request_id': 'request_id'
        },
        ExpressionAttributeValues: {
            ':request_id': requestId 
        }
    };

    client.query(params, (err, data) => {
        console.log('PUBLISH EVENT DATA');
        console.log(data);
        console.log(err);
        resolve({events: data.Items});
    });

});

const adminListPublishRequests = () => new Promise((resolve, reject) => {
    const client = new aws.DynamoDB.DocumentClient({
        region: 'us-west-2'
    });

    const params = {
        TableName: 'publish_requests',
        IndexName: 'status-index',
        KeyConditionExpression: '#status = :status',
        ExpressionAttributeNames: {
            '#status': 'status'
        },
        ExpressionAttributeValues: {
            ':status': 'APPROVED' 
        }
    };

    client.query(params, (err, data) => {
        console.log('PUBLISH QUERY DATA');
        console.log(data);
        console.log(err);
        resolve({requests: data.Items});
    });

});

const listPublishRequests = (gameId) => new Promise((resolve, reject) => {
    const client = new aws.DynamoDB.DocumentClient({
        region: 'us-west-2'
    });

    const params = {
        TableName: 'publish_requests',
        KeyConditionExpression: '#game_id = :game_id',
        ExpressionAttributeNames: {
            '#game_id': 'game_id'
        },
        ExpressionAttributeValues: {
            ':game_id': gameId
        }
    };

    client.query(params, (err, data) => {
        console.log('PUBLISH QUERY DATA');
        console.log(data);
        console.log(err);
        resolve({requests: data.Items});
    });

});

const listGamesForAuthor = ({ author, page, limit }) => new Promise((resolve, reject) => {

    const client = new aws.DynamoDB.DocumentClient({
        region: process.env.DYNAMO_REGION
    });

    const params = {
        TableName: 'games',
        IndexName: 'created_by_index',
        KeyConditionExpression: '#created_by = :created_by',
        ExpressionAttributeNames: {
            '#created_by': 'created_by'
        },
        ExpressionAttributeValues: {
            ':created_by': author
        }
    };

    client.query(params, (err, data) => {
        if (err) {
            reject([{error: err}]);
        } else {
            resolve(data.Items.map(mapGame));
        }
    });

});

const getGameDetails = (gameId) => new Promise((resolve, reject) => { 
    const client = new aws.DynamoDB.DocumentClient({
        region: process.env.DYNAMO_REGION
    });

    const params = {
        TableName: 'game_versions',
        // IndexName: 'name_index',
        KeyConditionExpression: '#game_id = :game_id',
        ExpressionAttributeNames: {
            '#game_id': 'game_id'
        },
        ExpressionAttributeValues: {
            ':game_id': gameId
        }
    };

    client.query(params, (err, data) => {
        if (err) {
            console.log(err);
            reject(err);
        } else {
            resolve({
                versions: data.Items.map(mapGameVersion)
            });
        }
    });
});

const mapGameVersion = (gameVersion) => {
    console.log('need to map this');
    console.log(gameVersion);
    return {
        version: gameVersion.version,
        publishedBy: gameVersion.published_by,
        'location': gameVersion['location'],
        description: gameVersion.description,
        versionId: gameVersion.version_id,
        publishedAt: gameVersion.published_at,
        commitHash: gameVersion.commit_hash,
        gameId: gameVersion.game_id
    };
};

const queryGames = (query) => new Promise((resolve, reject) => {
    const client = new aws.DynamoDB.DocumentClient({
        region: process.env.DYNAMO_REGION
    });

    const params = {
        TableName: 'games',
        IndexName: 'name_index',
        KeyConditionExpression: '#published_state = :public and begins_with(#name, :name)',
        ExpressionAttributeNames: {
            '#published_state': 'published_state',
            '#name': 'name'
        },
        ExpressionAttributeValues: {
            ':name': query,
            ':public': 'public'
        }
    };

    client.query(params, (err, data) => {
        if (err) {
            console.log(err);
            reject(err);
        } else {
            resolve(data.Items.map(mapGame));
        }
    });
});

const mapGame = (game) => {
    console.log('mapping game');
    console.log(game);
    return {
        createdBy: game.created_by && game.created_by.S ? game.created_by.S : game.created_by,
        createdAt: game.created_on && game.created_on.N ? game.created_on.N : game.created_on,
        id: game.game_id && game.game_id.S ? game.game_id.S : game.game_id,
        thumbnail: game.thumbnail && game.thumbnail.S ? game.thumbnail.S : game.thumbnail,
        name: game.name && game.name.S ? game.name.S : game.name,
        description: game.description && game.description.S ? game.description.S : game.description
    };
};

let gamesCache = {};

const listGames = (limit = 10, offset = 0, sort = DEFAULT_GAME_ORDER, query = null, tags = []) => new Promise((resolve, reject) => {
    if (gamesCache.timestamp && gamesCache.timestamp > Date.now() - (1 * 1000 * 60)) { //1 min
        console.log('returning from cache');
        resolve(gamesCache.pages);
    } else {
        console.log('fetching from dynamo');
        const client = new aws.DynamoDB.DocumentClient({
            region: process.env.DYNAMO_REGION
        });

        const queryParams = {
            TableName: 'games',
            Limit: 6,
            IndexName: 'name_index',
            KeyConditionExpression: '#published_state = :public',
            ExpressionAttributeNames: {
                '#published_state': 'published_state',
            },
            ExpressionAttributeValues: {
                ':public': 'public'
            }
        };

        // get data from dynamo in pages of 1MB by default or Limit if present
        // dumb and expensive because we can support more than Limit, but 1MB is a lot and is weird to get in one big blob, so paginate mostly for user experience
        // should use ES cache if we get a lot of traffic
        const pages = {};
        let pageCount = 1;

        const makeQueries = (lastResult) => new Promise((resolve, reject) => {

            client.query(queryParams, (err, data) => {
                console.log('got data');
                console.log(data);
                console.log(err);
                if (err) {
                    reject();
                } else {
                    if (data.Items.length) {
                        pages[pageCount++] = data.Items.map(mapGame);
                    }
                    if (data.LastEvaluatedKey) {
                        queryParams.ExclusiveStartKey = data.LastEvaluatedKey;
                        makeQueries(queryParams).then(resolve);
                    } else {
                        gamesCache = {pages, timestamp: Date.now()};
                        resolve(pages);
                    }
                }
					 
            });
	
        });

        makeQueries().then(resolve);
    }
});

const getGameInstance = (owner, repo, commit) => new Promise((resolve, reject) => {

    getCommit(owner, repo, commit).then(_res => {

        getBuild(owner, repo, commit).then((dir) => {
            const cmd = ['--prefix', dir.path, 'install'];
            const {
                exec
            } = require('child_process');
            exec('npm --prefix ' + dir.path + ' install', (err, stdout, stderr) => {
                const _game = require(dir.path);
                resolve(_game);
            });
        });

    });

});

const adminPublishRequestAction = (requestId, action, message) => new Promise((resolve, reject) => {
    const ddb = new aws.DynamoDB({
        region: 'us-west-2'
    });

    if (!action || action !== 'reject' && action !== 'approve') {
        reject('invalid action');
    }

    if (!message) {
        if (action === 'reject') {
            reject('rejection requires message');
        }

        message = 'No message available';
    }
	
    const newStatus = action === 'approve' ? 'PUBLISHED' : 'REJECTED';

    getPublishRequest(requestId).then(requestData => {
        console.log('got request data, need to do sometyhing to it');
        console.log(requestData);
        const sourceInfoHash = requestData.source_info_hash;
        const gameId = requestData.game_id;

        const updateParams = {
            TableName: 'publish_requests',
            Key: {
                'game_id': {
                    S: gameId
                },
                'source_info_hash': {
                    S: sourceInfoHash
                }
            },
            AttributeUpdates: {
                'status': {
                    Action: 'PUT',
                    Value: {
                        S: newStatus
                    }
                },
                'adminMessage': {
                    Action: 'PUT',
                    Value: {
                        S: message
                    }
                }
            }
        };

        ddb.updateItem(updateParams, (err, putResult) => {
            console.log(err);
            if (err) {
                reject();
            } else {
                resolve();
            }
        });
    });

});

const server = http.createServer((req, res) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Headers', '*');

    if (req.method === 'GET') {

        const publishRequestsRegex = new RegExp('/games/(\\S*)/publish_requests');
        const publishRequestEventsRegex = new RegExp('/publish_requests/(\\S*)/events');
        const gameDetailRegex = new RegExp('/games/(\\S*)');
        const gameVersionDetailRegex = new RegExp('/games/(\\S*)/version/(\\S*)');
        if (req.url === '/admin/publish_requests') {
            console.log('yoooooo');
            const username = req.headers['hg-username'];
            const token = req.headers['hg-token'];

            verifyAccessToken(username, token).then(() => {
                console.log('verified access token');
                getCognitoUser(username).then(userData => {
                    if (userData.isAdmin) {
                        console.log(userData);
                        adminListPublishRequests().then(publishRequests => {
                            console.log('PUBLISH REQUESTSTS');
                            console.log(publishRequests);
                            res.end(JSON.stringify(publishRequests));
                        }).catch(err => {
                            console.log('failed to list publish requests');
                            console.error(err);
                            res.end('failed to list requests');
                        });
                    } else {
                        console.log('user attempted to call admin API: ' + username);
                        res.end('user is not an admin');
                    }
                }).catch(err => {
                    console.log(err);
                    res.end('failed to get user data');
                });
            }).catch(err => {
                console.log('admin access token error');
                console.log(err);
                res.end('admin access token error');
            });
        } else if (req.url == '/assets') {
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

                        res.end(JSON.stringify({
                            assets
                        }));
                    });
                }).catch((err) => {
                    console.log(err);
                    res.end('error');
                });
            }
        } else if (req.url.startsWith('/tags')) {
            const queryObject = url.parse(req.url, true).query;
            const userFilter = queryObject.user;

            if (userFilter) {
                const username = req.headers['hg-username'];
                const token = req.headers['hg-token'];

                if (!username || !token) {
                    res.end('no');
                } else {
                    if (username !== userFilter) {
                        res.end('not matching');
                    } else {
                        verifyAccessToken(username, token).then(() => {
                            getTags(null, userFilter).then(tags => {
                                res.end(JSON.stringify({
                                    tags
                                }));
                            });
                        });
                    }
                }
            } else {
                const tagQuery = queryObject.query;
                getTags(null, null, tagQuery).then(tags => {
                    res.end(JSON.stringify({
                        tags
                    }));
                });
            }
        } else if (req.url.startsWith('/verify_publish_request')) {	
            const queryObject = url.parse(req.url, true).query;
            const { code, requestId } = queryObject;
            verifyPublishRequest(code, requestId).then((publishRequest) => {
                publishGameVersion(publishRequest);
                res.end('cool thanks');
            }).catch(err => {
                res.end(err);
            });
        } else if (req.url.startsWith('/confirm')) {
            const queryObject = url.parse(req.url, true).query;

            const code = queryObject.code;
            const commit = queryObject.commit;

            const ddb = new aws.DynamoDB({
                region: 'us-west-2'
            });

            const params = {
                TableName: 'hg_requests',
                Key: {
                    'code': {
                        S: code
                    },
                    'commit': {
                        S: commit
                    }
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
                            'game_id': {
                                S: gameId
                            },
                            'version': {
                                N: '' + version
                            }
                        }
                    }, (err, data) => {
                        console.log('got data');
                        console.log(data);

                        const updateParams = {
                            TableName: 'hg_game_versions',
                            Key: {
                                'game_id': {
                                    S: gameId
                                },
                                'version': {
                                    N: '' + version
                                }
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
                                'code': {
                                    S: code
                                },
                                'commit': {
                                    S: commit
                                }
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

        } else if (req.url.match(publishRequestEventsRegex)) {
            const username = req.headers['hg-username'];
            const token = req.headers['hg-token'];

            if (!username || !token) {
                res.end('no');
            } else {
                verifyAccessToken(username, token).then(() => {
                    const gameId = publishRequestEventsRegex.exec(req.url)[1].split('?')[0];
                    getPublishRequestEvents(gameId).then((publishRequests) => {
                        res.end(JSON.stringify(publishRequests));
                    });
                });
            }
	
        } else if (req.url.match(publishRequestsRegex)) {
            const username = req.headers['hg-username'];
            const token = req.headers['hg-token'];

            if (!username || !token) {
                res.end('no');
            } else {
                const gameId = publishRequestsRegex.exec(req.url)[1].split('?')[0];
                verifyAccessToken(username, token).then(() => {
                    listPublishRequests(gameId).then((publishRequests) => {
                        res.end(JSON.stringify(publishRequests));
                    });
                });
            }
        } else if (req.url.match(gameVersionDetailRegex)) {
            const match = gameVersionDetailRegex.exec(req.url);
            console.log('MATCH');
            console.log(match);
            const gameId = match[1].split('?')[0];
            const versionId = match[2].split('?')[0];
            // maybe make separate index for verison ID to fetch directly at some point
            getGameDetails(gameId).then(data => {
                console.log('got game details, looking for ' + versionId);
                console.log(data);
                const foundVersion = data.versions.find(v => v.versionId === versionId);
                if (!foundVersion) {
                    res.end('Version not found');
                } else {
                    res.end(JSON.stringify(foundVersion));
                }
            }).catch((err) => {
                console.log('game detail fetch err');
                console.log(err);
                res.end('Game not found');
            });
        } else if (req.url.match(gameDetailRegex)) {
            const gameId = gameDetailRegex.exec(req.url)[1].split('?')[0];

            getGameDetails(gameId).then(data => {
                res.writeHead(200, {
                    'Content-Type': 'application/json'
                });
                res.end(JSON.stringify(data)); 
            }); 
        } else if (req.url.startsWith('/games')) {
            const queryObject = url.parse(req.url, true).query;
            const { query, author, page, limit } = queryObject;
            if (author) {
                console.log('you want games by author ' + author);
                // verifyAccessToken(username, token).then(() => {
                listGamesForAuthor({ author, page, limit }).then((data) => {
                    res.writeHead(200, {
                        'Content-Type': 'application/json'
                    });
                    res.end(JSON.stringify({
                        games: data
                    }));
                });
                // });
            } else if (query) {
                queryGames(query).then(data => {
                    res.writeHead(200, {
                        'Content-Type': 'application/json'
                    });
                    res.end(JSON.stringify({
                        games: data
                    })); 
                });
            } else {
                listGames().then(pages=> {
                    res.writeHead(200, {
                        'Content-Type': 'application/json'
                    });
                    console.log('got pages');
                    console.log(pages);
                    res.end(JSON.stringify({
				 games: pages[page || 1],
				 pageCount: Object.keys(pages).length
                    })); 
                });
            }
        } else {
            res.end('ok');
        }
    } else if (req.method === 'POST') {
        const gamePublishRegex = new RegExp('/games/(\\S*)/publish');
        const gameUpdateRegex = new RegExp('/games/(\\S*)/update');
        const requestActionRegex = new RegExp('/admin/request/(\\S*)/action');
        if (req.url.match(requestActionRegex)) {
            const username = req.headers['hg-username'];
            const token = req.headers['hg-token'];
            verifyAccessToken(username, token).then(() => {
                console.log('verified access token');
                getCognitoUser(username).then(userData => {
                    const _requestActionRegex = new RegExp('/admin/request/(\\S*)/action');
                    const requestId = _requestActionRegex.exec(req.url)[1];
                    if (userData.isAdmin) {
                        getReqBody(req, (_data) => {
                            const reqBody = JSON.parse(_data);
                            if (reqBody.action) {
                                console.log('need to perform action on request ' + requestId);
                                adminPublishRequestAction(requestId, reqBody.action, reqBody.message).then(() => {
                                    res.end('approved!');
                                }).catch(err => {
                                    console.log('error performing publish action');
                                    console.log(err);
                                    res.end('error performing publish request action');
                                });
                            } else {
                                res.end('request missing action');
                            }
                        });
                    } else {
                        console.log('user ' + username + ' attempted to perform action on request');
                        res.end('not an admin');
                    }
                }).catch(err => {
                    console.log('failed to get user data');
                    console.log(err);
                    res.end('could not get user data');
                });
            }).catch(err => {
                console.log('failed to verify access token');
                res.end('failed to verify access token');
            });
        } else if (req.url.match(gameUpdateRegex)) {
            const username = req.headers['hg-username'];
            const token = req.headers['hg-token'];

            if (!username || !token) {
                res.end('no');
            } else {
                verifyAccessToken(username, token).then(() => {
                    const form = new multiparty.Form();

                    getReqBody(req, (_data) => {
                        const parsedData = querystring.decode(_data);
                        console.log('parsed data');
                        console.log(parsedData);
                        const client = new aws.DynamoDB({
                            region: 'us-west-2'
                        });
                        const readClient = new aws.DynamoDB.DocumentClient({
                            region: 'us-west-2'
                        });

                        const _gameUpdateRegex = new RegExp('/games/(\\S*)/update');
                        const gameId = _gameUpdateRegex.exec(req.url)[1];
                        console.log('developer ' + username + ' wants to update ' + gameId);
                        console.log(data);
                        const changed = data.description || data.thumbnail;

                        if (changed) {
                            getGame(gameId, username).then(game => {
                                console.log('about to update');
                                if (username != game.author) {
                                    res.writeHead(400, {
                                        'Content-Type': 'text/plain'
                                    });
                                    res.end('You cannot modify a game that you didnt create');
                                } else {
                                    const newVersion = '' + (Number(game.version) + 1);
                                    if (data.description != game.description) {
                                        updateGame(username, gameId, game.name, data.description, newVersion).then((_game) => {
                                            // sigh. 
                                            setTimeout(() => {
                                                res.writeHead(200, {
                                                    'Content-Type': 'application/json'
                                                });
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
                            res.writeHead(400, {
                                'Content-Type': 'text/plain'
                            });
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
                            console.log('create a tag');
                            console.log(data);
                            tagGame(data.tag, data.game_id, username).then(() => {
                                res.writeHead(200, {
                                    'Content-Type': 'application/json'
                                });
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
            console.log('publishing game');	
            const username = req.headers['hg-username'];
            const token = req.headers['hg-token'];

            if (!username || !token) {
                res.end('no');
            } else {
                verifyAccessToken(username, token).then(() => {
                    const form = new multiparty.Form();

                    getReqBody(req, (_data) => {
                        const data = JSON.parse(_data);
                        const _gamePublishRegex = new RegExp('/games/(\\S*)/publish');
                        const gameId = _gamePublishRegex.exec(req.url)[1];

                        const publishData = {
                            commit: data.commit,
                            requester: username,
                            owner: data.owner,
                            repo: data.repo,
                            gameId
                        };

                        console.log('publishing with payload');
                        console.log(publishData);

                        const buildSourceInfoHash = ({sourceType, commit, owner, repo}) => {
                            const stringConcat = `${sourceType}${commit}${owner}${repo}`;
                            console.log('hashing string: ' + stringConcat);
                            return crypto.createHash('md5').update(stringConcat).digest('hex');
                        };

                        const verifyNoExistingPublishRequest = ({commit, owner, repo, gameId, requester}) => new Promise((resolve, reject) => {

                            const sourceInfoHash = buildSourceInfoHash({sourceType: SourceType.GITHUB, commit, owner, repo});

                            console.log('source info hash ' + sourceInfoHash);

                            const readClient = new aws.DynamoDB.DocumentClient({
                                region: 'us-west-2'
                            });

                            const readParams = {
                                TableName: 'publish_requests',
                                Key: {
                                    'game_id': gameId,
                                    'source_info_hash': sourceInfoHash
                                }
                            };

                            readClient.get(readParams, (err, data) => {
                                console.log(err);
                                console.log(data);
					
                                // todo: handle error separately
                                if (err || data.Item) {
                                    reject();
                                } else {
                                    resolve();
                                }
                            });
                        });

                        const createPublishRequest = ({commit, owner, repo, gameId, requester}) => new Promise((resolve, reject) => {
			
                            const sourceInfoHash = buildSourceInfoHash({sourceType: SourceType.GITHUB, commit, owner, repo});

                            const client = new aws.DynamoDB({
                                region: 'us-west-2'
                            });

                            const requestId = `${gameId}:${sourceInfoHash}`;

                            const params = {
                                TableName: 'publish_requests',
                                Item: {
                                    'request_id': {
                                        S: requestId
                                    },
                                    'commit_hash': {
                                        S: commit
                                    },
                                    'repo_owner': {
                                        S: owner
                                    },
                                    'repo_name': {
                                        S: repo
                                    },
                                    'game_id': {
                                        S: gameId
                                    },
                                    'requester': {
                                        S: requester
                                    }, 
                                    'source_info_hash': {
                                        S: sourceInfoHash
                                    },
                                    'created': {
                                        N: `${Date.now()}`
                                    },
                                    'status': {
                                        S: 'SUBMITTED'
                                    }
                                }
                            };
			
                            client.putItem(params, (err, putResult) => {
                                if (!err) {
                                    resolve({sourceInfoHash, gameId});
                                } else {
                                    reject(err);
                                }
                            });
                        });

                        verifyNoExistingPublishRequest(publishData).then(() => {
                            createPublishRequest(publishData).then((publishRecord) => {
                                console.log('want to publish game but just going to put an sqs message');
                                console.log(publishData);
                                console.log('record');
                                console.log(publishRecord);
                                const messageBody = JSON.stringify(publishRecord);
                                //
                                const sqsParams = {
                                    MessageBody: messageBody,
                                    QueueUrl: QUEUE_URL,
                                    MessageGroupId: Date.now() + '',
                                    MessageDeduplicationId: publishRecord.sourceInfoHash
                                };
                                //			   
                                const sqs = new aws.SQS({region: 'us-west-2'});
                                //
                                sqs.sendMessage(sqsParams, (err, sqsResponse) => {
                                    console.log('sent message');
                                    console.log(err);
                                    console.log(sqsResponse);
                                    res.end('created publish request');
                                });
                            }).catch((err) => {
                                console.error('sqs error');
                                console.error(err);
                                res.writeHead(500);
                                res.end('Failed to create publish request');
                            });
                        }).catch(() => {
                            res.writeHead(400);
                            res.end('Publish request already exists');
                        });
                    });
                }).catch((err) => {
                    console.log('failed to verify access token');
                    console.log(err);
                    res.end('failed to verify access token');
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

                                        fork(path.join(__dirname, 'upload.js'),
                                            [
                                                `--path=${f.path}`,
                                                `--developer=${username}`,
                                                `--id=${assetId}`,
                                                `--name=${f.originalFilename}`,
                                                `size=${f.size}`,
                                                `type=${f.headers['content-type']}`
                                            ]
                                        );
                                        res.writeHead(200, {
                                            'Content-Type': 'application/json'
                                        });
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

                    const form = new multiparty.Form();
                    form.parse(req, (err, fields, files) => {
                        if (err) {
                            console.error('error parsing form');
                            console.error(err);
                            res.end('error');
                        } else {
                            console.log('files and fields');
                            console.log(files);
                            console.log(fields);
                            if (!files.thumbnail || !files.thumbnail.length || !fields.name || !fields.name.length || !fields.description || !fields.description.length) {
                                res.end('creation requires name, thumbnail & description');
                            } else {
                                console.log('ned to upload to s3 then create in dynamo');
                                const gameId = generateId();
                                uploadThumbnail(username, gameId, files.thumbnail[0]).then((url) => {
                                    const nowString = '' + Date.now();
                                    const params = {
                                        RequestItems: {
                                            [process.env.GAME_TABLE]: [{
                                                PutRequest: {
                                                    Item: {
                                                        'game_id': {
                                                            S: gameId
                                                        },
                                                        'created_by': {
                                                            S: username
                                                        },
                                                        'name': {
                                                            S: fields.name[0]
                                                        },
                                                        'created_on': {
                                                            N: nowString
                                                        },
                                                        'updated': {
                                                            N: nowString
                                                        },
                                                        'description': {
                                                            S: fields.description[0] 
                                                        },
                                                        'thumbnail': {
                                                            S: url
                                                        }
                                                    }
                                                }
                                            }]
                                        }
                                    };
	
                                    const client = new aws.DynamoDB({
                                        region: 'us-west-2'
                                    });
                                    client.batchWriteItem(params, (err, putResult) => {
	
                                        console.log(err);
                                        console.log(putResult);
	
                                        if (!err) {
                                            res.writeHead(200, {
                                                'Content-Type': 'application/json'
                                            });
                                            res.end(JSON.stringify(mapGame(params.RequestItems[process.env.GAME_TABLE][0].PutRequest.Item)));
                                        } else {
                                            console.log(err);
                                            res.end('error');
                                        }
                                    });

                                });
                            }
                        }
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
