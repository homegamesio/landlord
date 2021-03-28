const aws = require('aws-sdk');
const { v4: uuidv4 } = require('uuid');
const crypto = require('crypto');
const fs = require('fs');

const _filePath = process.argv[2].replace('--path=', '');
const _developerId = process.argv[3].replace('--developer=', '');
const _assetId = process.argv[4].replace('--id=', '');
const _fileName = process.argv[5].replace('--name=', '');
const _fileSize = process.argv[6].replace('--size=', '');
const _fileType = process.argv[7].replace('--type=', '');

const getHash = (input) => {
    return crypto.createHash('md5').update(input).digest('hex');
};

const upload = (filePath, assetId, fileName, fileSize, fileType) => new Promise((resolve, reject) => {
    console.log('need to upload with ' + assetId);
    const s3 = new aws.S3({region: 'us-west-2'});

    fs.readFile(filePath, (err, data) => {
        const upload = new aws.S3.ManagedUpload({
            params: {
                ACL: 'public-read',
                //Metadata: {
                    ContentType: fileType,
                    ContentDisposition: `attachment; filename=${fileName}`,
                //},
                Bucket: 'assets.homegames.io',
                Key: assetId,
                Body: fs.readFileSync(filePath)
            }   
        });

        upload.promise().then(response => {
            resolve();
        });
    });
});

const updateRecord = (developerId, assetId, _status) => new Promise((resolve, reject) => {
    const client = new aws.DynamoDB({region: 'us-west-2'});
    const params = {
        TableName: 'homegames_assets',
        Key: {
            'developer_id': {S: developerId},
            'asset_id': {S: assetId}
        },
        AttributeUpdates: {
            'status': {
                Action: 'PUT', 
                Value: {
                    S: _status
                }
            },
            'updated_at': {
                Action: 'PUT',
                Value: {
                    N: '' + Date.now()
                }
            }
        }
    };

    client.updateItem(params, (err, putResult) => {
        if (!err) {
            resolve();
        } else {
            reject(err);
        }
    });

});


updateRecord(_developerId, _assetId, 'processing').then(() => {
    console.log('created record for ' + _fileName);
    upload(_filePath, _assetId, _fileName, _fileSize, _fileType).then(() => {
        console.log('done uploading');
        updateRecord(_developerId, _assetId, 'complete').then(() => {
            console.log('finished!');
        });

    });
});
