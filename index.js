const http = require('http');
const https = require('https');
const unzipper = require('unzipper');
const url = require('url');
const archiver = require('archiver');
const fs = require('fs');
const aws = require('aws-sdk');
const {
    v4: uuidv4
} = require('uuid');
const crypto = require('crypto');
const util = require('util');
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
	GITHUB: "GITHUB"
};

const getLatestGameVersion = (gameId) => new Promise((resolve, reject) => {
	const readClient = new aws.DynamoDB.DocumentClient({
            region: 'us-west-2'
        });

        console.log(gameId);
        const params = {
            TableName: 'hg_game_versions',
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
		    		    TableName: 'hg_game_versions',
		    		    Item: {
		    		        'version': {
				    	    	N: '' + newVersion
		    		        },
					'location': {
						S: s3Url
					},
					'created': {
						N: '' + Date.now()
					},
					'request_id': {
						S: requestId
					},
					'game_id': {
						S: gameId
					}
		    		    }
		    		};
		
		    		client.putItem(params, (err, putResult) => {
		    		    if (!err) {
					    console.log('published new game version of game id ' + gameId);
					    console.log('now need to update latest approved version of game');
				    		updateGame(publishRequest.requester, gameId, gameData.name, gameData.description, newVersion).then(() => {
							console.log('set latest approved verion');
		    		        		resolve();
						}).catch(err => {
							console.log('failed to set latest approved version');
							console.log(err);
							reject();
						});

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
				console.log("got request");
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
							console.log("updated status");
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

const getTags = (__gameId, __userId, __query, offset = 0, limit = 10) => new Promise((__resolve, __reject) => {
    const _data = {
        aggs: {
            tags: {
                terms: {
                    field: 'tag_id'
                }
            }
        },
        from: offset,
        size: limit
    };

    if (__gameId) {
        _data.query = {
            term: {
                'game_id': {
                    value: __gameId
                }
            }
        };
    } else if (__userId) {
        _data.query = {
            term: {
                'user_id': {
                    value: __userId
                }
            }
        }
    } else if (__query) {
    _data.query = {
        match: {
            'tag_id': {
                query: __query,
                fuzziness: "AUTO"
            }
        }
    };
    }
    
    const __data = JSON.stringify(_data);

    console.log("TAG QUERY");
    console.log(__data);

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
                const __results = __buf.aggregations.tags.buckets.map(item => item.key); //mapGame(hit._source));
                __resolve(__results);
            } else {
                __resolve([]);
            }

        });
    });

    __req.write(__data);

    __req.end();
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

const getGame = (gameId, developerId, version = null) => new Promise((resolve, reject) => {

	const compositeString = version ? `${version}:${gameId}` : `latest:${gameId}`;

        const client = new aws.DynamoDB({
            region: 'us-west-2'
        });

        const params = {
            TableName: 'hg_games',
            Key: {
		    'developer_id': {
			S: developerId
		    },
		    'game_composite': {
			S: compositeString
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
	console.log("updating game: " + gameId);

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

const doSearch = (searchQuery) => new Promise((resolve, reject) => {
    const data = JSON.stringify({
        query: {
            multi_match: {
                fields: ['game_name', 'author', 'description'],
                query: searchQuery,
                fuzziness: 'AUTO'
            }
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

const DEFAULT_GAME_ORDER = {
    'game_name': {
        order: 'asc'
    }
};

const getPublishRequestEvents = (requestId) => new Promise((resolve, reject) => {
	console.log("you want publish request events for this request! " + requestId);
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
		    console.log("PUBLISH EVENT DATA");
		    console.log(data);
		    console.log(err);
		    resolve({events: data.Items});
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
		    console.log("PUBLISH QUERY DATA");
		    console.log(data);
		    console.log(err);
		    resolve({requests: data.Items});
            });

});

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
                            must: [{
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

const server = http.createServer((req, res) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Headers', '*');

    if (req.method === 'GET') {

        const publishRequestsRegex = new RegExp('/games/(\\S*)/publish_requests');
        const publishRequestEventsRegex = new RegExp('/publish_requests/(\\S*)/events');
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
	} else if (req.url.match(gameDetailRegex)) {
            const gameId = gameDetailRegex.exec(req.url)[1].split('?')[0];

            const client = new aws.DynamoDB.DocumentClient({
                region: 'us-west-2'
            });

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


                getTags(gameId).then(tags => {

                    console.log('GGGG TAGS');
                    console.log(tags);
                    res.writeHead(200, {
                        'Content-Type': 'application/json'
                    });
                    res.end(JSON.stringify({
                        tags,
                        versions: results
                    }));
                });

            });
        } else if (req.url.startsWith('/games')) {
            		const client = new aws.DynamoDB.DocumentClient({
            		    region: 'us-west-2'
            		});
            		const queryObject = url.parse(req.url, true).query;

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

            		            res.end(JSON.stringify({
            		                games: gameList
            		            }));
            		        } else {
            		            res.end({
            		                error: err
            		            });
            		        }
            		    });
            		} else if (searchQuery) {
            		    doSearch(searchQuery).then(d => {
            		        res.end(JSON.stringify({
            		            games: d
            		        }));
            		    });
            		} else {
            		    listGames(10, 0, DEFAULT_GAME_ORDER, searchQuery, tags ? tags.split(',') : []).then(d => {
            		        console.log('GAMES');
            		        console.log(d);
            		        res.end(JSON.stringify({
            		            games: d
            		        }));
            		    });
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
                            console.log("create a tag");
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
						"game_id": gameId,
						"source_info_hash": sourceInfoHash
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
				}).catch(() => {
					res.writeHead(500);
					res.end('Failed to create publish request');
				})
			}).catch(() => {
				res.writeHead(400);
				res.end('Publish request already exists');
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
                    getReqBody(req, (_data) => {
                        console.log('sdd');
                        const data = JSON.parse(_data);

                        const readClient = new aws.DynamoDB.DocumentClient({
                            region: 'us-west-2'
                        });

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
                                    res.writeHead(400, {
                                        'Content-Type': 'text/plain'
                                    });
                                    res.end('that already exists');
                                } else {
                                    const gameId = generateGameId();
                                    const nowString = '' + Date.now();
                                    const descriptionString = ('' + data.description) || 'No description provided';
                                    console.log('wat');
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
                                                                S: username
                                                            },
                                                            'game_name': {
                                                                S: data.game_name
                                                            },
                                                            'created': {
                                                                N: nowString
                                                            },
                                                            'version': {
                                                                N: '1'
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
                                                                S: `${gameId}:1`
                                                            },
                                                            'game_id': {
                                                                S: gameId
                                                            },
                                                            'developer_id': {
                                                                S: username
                                                            },
                                                            'game_name': {
                                                                S: data.game_name
                                                            },
                                                            'created': {
                                                                N: nowString
                                                            },
                                                            'version': {
                                                                N: '1'
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
