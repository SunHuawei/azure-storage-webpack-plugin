var PromisePool = require("es6-promise-pool");
var mime = require('mime');
var utils = require('./lib/utils');
var md5File = require('md5-file');
var blobServiceFactor = require('./lib/blobService');

var assetChecksums = {};

function apply(options, compiler) {
  // When assets are being emmited (not yet on file system)
  var blobService = blobServiceFactor(options.blobService);
  compiler.plugin('after-emit', function (compilation, callback) {
    blobService.createContainerIfNotExists(options.container.name,  options.container.options, function(error, response) {
      if(error) {
        console.log("Error on createContainerIfNotExists '" + options.container.name + "'");
        console.error(error);
        return;
      }

      function handleFiles(files) {
        let index = 0;
        return new PromisePool(() => {
          const currentIndex = index++;
          const file = files[currentIndex];
          if (currentIndex >= files.length) {
            return null;
          } else {
            return new Promise(function(resolve, reject) {
              md5File(file.path, function(error, md5sum) {
                if (error) {
                  console.log("Error computing md5sum for '" + file.path + "'");
                  console.error(error);
                  reject(error);
                  return;
                }

                var lastChecksum = assetChecksums[file.path];
                if (!process.env.ALWAYS_UPLOAD && lastChecksum === md5sum) {
                  console.log("skipping upload of '" + file.path + "' (current MD5 checksum matches last uploaded MD5 checksum)");
                  resolve();
                  return;
                }

                var metadata = Object.assign({
                  contentType: mime.getType(file.path)
                }, options.metadata);
                var opts = { metadata: metadata };
                var name = options.path ? options.path + "/" + file.name : file.name;

                blobService.checkBlockBlob(options.container.name, name, file.path, function(error, isExisting, isExact) {
                  if(error) {
                    console.log("Error on checkBlockBlob for '" + file.path + "'");
                    console.error(error);
                    reject(error);
                    return;
                  }

                  function uploadHandler(error) {
                    if (error) {
                      console.log("Error on uploadHandler for '" + file.path + "'");
                      console.error(error);
                      reject(error);
                      return;
                    }

                    resolve();
                  }

                  if (isExact) {
                    console.log("skip this existing blob '" + file.path + "' in container '" + options.container.name + "'");
                    resolve();
                  }  else if (isExisting) {
                    // options.overwrite, true as default
                    if (options.overwrite === false) {
                      uploadFileToBlockBlob(blobService, options, name, file, opts, md5sum, uploadHandler);
                    } else {
                      console.warn("same name without same content for '" + file.path + "' in container '" + options.container.name + "'");
                      resolve();
                    }
                  } else {
                    uploadFileToBlockBlob(blobService, options, name, file, opts, md5sum, uploadHandler);
                  }
                });
              });
            });
          }
        }, options.concurrency > 0 ? options.concurrency : 10).start();
      }

      if (options.directory) {
        utils.getDirectoryFilesRecursive(options.directory).then(handleFiles).then(function() {
          console.log("Done upload to azure blob!")
        }).then(callback);
      } else {
        handleFiles(utils.getAssetFiles(compilation)).then(function() {
          console.log("Done upload to azure blob!")
        });
        callback();
      }
    });
  });
}

function uploadFileToBlockBlob(blobService, options, name, file, opts, md5sum, callback) {
  blobService.createBlockBlobFromLocalFile(options.container.name, name, file.path, opts, function (error, url, response) {
    if (error) {
      console.log("Error on createBlockBlobFromLocalFile for '" + file.path + "'");
      console.error(error);
      callback(error);
      return;
    }

    assetChecksums[file.path] = md5sum;
    if (!process.env.SILENCE_UPLOADS) {
      console.log("successfully uploaded '" + file.path + "' to '" + url + "'");
    }

    callback(null);
  });
}

function AzureStorageDeployWebpackPlugin(options) {
  // Simple pattern to be able to easily access plugin
  // options when the apply prototype is called
  return {
    apply: apply.bind(this, options)
  };
}

module.exports = AzureStorageDeployWebpackPlugin;
