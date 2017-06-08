const AWS = require('aws-sdk');
const s3 = new AWS.S3();

const conf = require('./config.js');
const fs = require('fs');
const path = require('path');
const fetch = require('node-fetch');
const crypto = require('crypto');

const redis = require('redis');
const redis_client = redis.createClient({
  host: conf.redis_host
});

redis_client.on('error', err => {
  console.log(err);
});

const STATIC_DIR = path.join(__dirname, '..', 'static');

const notLocalhost =
  conf.env === 'production' &&
  conf.s3_bucket !== 'localhost' &&
  conf.bitly_key !== 'localhost';

if (notLocalhost) {
  module.exports = {
    filename: filename,
    length: awsLength,
    get: awsGet,
    set: awsSet,
    delete: awsDelete,
    forceDelete: awsForceDelete
  };
} else {
  module.exports = {
    filename: filename,
    length: localLength,
    get: localGet,
    set: localSet,
    delete: localDelete,
    forceDelete: localForceDelete
  };
}

function filename(id) {
  return new Promise((resolve, reject) => {
    redis_client.hget(id, 'filename', (err, reply) => {
      if (!err) {
        resolve(reply);
      } else {
        reject();
      }
    });
  });
}

function localLength(id) {
  return new Promise((resolve, reject) => {
    try {
      resolve(fs.statSync(path.join(STATIC_DIR, id)).size);
    } catch (err) {
      reject();
    }
  });
}

function localGet(id) {
  return fs.createReadStream(path.join(STATIC_DIR, id));
}

function localSet(id, file, filename, url) {
  return new Promise((resolve, reject) => {
    fstream = fs.createWriteStream(path.join(STATIC_DIR, id));
    file.pipe(fstream);
    fstream.on('close', () => {
      let uuid = crypto.randomBytes(10).toString('hex');

      redis_client.hmset([id, 'filename', filename, 'delete', uuid]);
      redis_client.expire(id, 86400000);
      console.log('Upload Finished of ' + filename);
      resolve({
        uuid: uuid,
        url: url
      });
    });

    fstream.on('error', () => reject());
  });
}

function localDelete(id, delete_token) {
  return new Promise((resolve, reject) => {
    redis_client.hget(id, 'delete', (err, reply) => {
      if (!reply || delete_token !== reply) {
        reject();
      } else {
        redis_client.del(id);
        resolve(fs.unlinkSync(path.join(STATIC_DIR, id)));
      }
    });
  });
}

function localForceDelete(id) {
  return new Promise((resolve, reject) => {
    redis_client.del(id);
    resolve(fs.unlinkSync(path.join(STATIC_DIR, id)));
  });
}

function awsLength(id) {
  let params = {
    Bucket: conf.s3_bucket,
    Key: id
  };
  return new Promise((resolve, reject) => {
    s3.headObject(params, function(err, data) {
      if (!err) {
        resolve(data.ContentLength);
      } else {
        reject();
      }
    });
  });
}

function awsGet(id) {
  let params = {
    Bucket: conf.s3_bucket,
    Key: id
  };

  return s3.getObject(params).createReadStream();
}

function awsSet(id, file, filename, url) {
  const params = {
    Bucket: conf.s3_bucket,
    Key: id,
    Body: file
  };
  const upload = util.promisify(s3.upload);

  return upload(params)
    .then(() => {
      const uuid = crypto.randomBytes(10).toString('hex');
      redis_client.hmset([id, 'filename', filename, 'delete', uuid]);
      if (conf.bitly_key) {
        return fetch(bitlyUrl(conf.bitly_key, url))
          .then(res => res.text())
          .then(short_url => ({ uuid, url: short_url }));
      }
      return { uuid, url };
    });

  return new Promise((resolve, reject) => {
    s3.upload(params, function(err, data) {
      if (err) {
        console.log(err, err.stack); // an error occurred
        reject();
      } else {
        let uuid = crypto.randomBytes(10).toString('hex');

        redis_client.hmset([id, 'filename', filename, 'delete', uuid]);

        function bitlyUrl(key, url) {
          return `https://api-ssl.bitly.com/v3/shorten?access_token=${key}&longUrl=${encodeURIComponent(url)}&format=txt`;
        }

        redis_client.expire(id, 86400000);
        console.log('Upload Finished of ' + filename);
        if (conf.bitly_key) {
          fetch(bitlyUrl(conf.bitly_key, url))
            .then(res => {
              return res.text();
            })
            .then(body => {
              resolve({
                uuid: uuid,
                url: body
              });
            });
        } else {
          resolve({
            uuid: uuid,
            url: url
          });
        }
      }
    });
  });
}

function awsDelete(id, delete_token) {
  return new Promise((resolve, reject) => {
    redis_client.hget(id, 'delete', (err, reply) => {
      if (!reply || delete_token !== reply) {
        reject();
      } else {
        redis_client.del(id);
        let params = {
          Bucket: conf.s3_bucket,
          Key: id
        };

        s3.deleteObject(params, function(err, data) {
          resolve(err);
        });
      }
    });
  });
}

function awsForceDelete(id) {
  return new Promise((resolve, reject) => {
    redis_client.del(id);
    let params = {
      Bucket: conf.s3_bucket,
      Key: id
    };

    s3.deleteObject(params, function(err, data) {
      resolve(err);
    });
  });
}
