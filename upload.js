const aws = require('aws-sdk');
const { v4: uuidv4 } = require('uuid');
const crypto = require('crypto');
const fs = require('fs');

const _filePath = process.argv[2].replace('--path=', '');
const _assetId = process.argv[3].replace('--id=', '');
const _fileName = process.argv[4].replace('--name=', '');
const _fileSize = process.argv[5].replace('--size=', '');
const _fileType = process.argv[6].replace('--type=', '');

const getHash = (input) => {
    return crypto.createHash('md5').update(input).digest('hex');
};

const upload = (filePath, assetId, fileName, fileSize, fileType) => {
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
            console.log("RESPONSE");
            console.log(response)
        });
    });
};

upload(_filePath, _assetId, _fileName, _fileSize, _fileType);
