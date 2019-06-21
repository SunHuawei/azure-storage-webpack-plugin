var md5File = require("md5-file");
var {
  Aborter,
  BlobURL,
  BlockBlobURL,
  ContainerURL,
  uploadFileToBlockBlob,
  ServiceURL,
  StorageURL,
  SharedKeyCredential
} = require("@azure/storage-blob");

module.exports = function(credential) {
  var account = credential[0];
  var accountKey = credential[1];
  var sharedKeyCredential = new SharedKeyCredential(account, accountKey);

  var pipeline = StorageURL.newPipeline(sharedKeyCredential);

  var serviceURL = new ServiceURL(
    `https://${account}.blob.core.windows.net`,
    pipeline
  );

  return {
    createContainerIfNotExists: function(containerName, options, callback) {
      var containerURL = ContainerURL.fromServiceURL(serviceURL, containerName);
      containerURL.getProperties(Aborter.none)
        .then(function(response) {
          callback(null, response);
        })
        .catch(function(error) {
          if (error.statusCode === 404) {
            containerURL.create(Aborter.none, options)
              .then(function(response) {
                callback(null, response);
              })
              .catch(function(error) {
                callback(error, null);
              });
          } else {
            callback(error, null);
          }
        });
    },

    checkBlockBlob: function(containerName, blobName, localFilePath, callback) {
      var containerURL = ContainerURL.fromServiceURL(serviceURL, containerName);
      var blobURL = BlobURL.fromContainerURL(containerURL, blobName);
      blobURL
        .getProperties(Aborter.none)
        .then(function(response) {
          var remoteMD5 = response.contentMD5.toString("hex");
          if (remoteMD5) {
            md5File(localFilePath, function(error, md5sum) {
              if (error) {
                console.log(
                  "Error computing md5sum for '" + localFilePath + "'"
                );
                console.error(error);
                callback(error);
                return;
              }

              callback(null, true, md5sum === remoteMD5);
            });
          } else {
              callback(null, false, false);
          }
        })
        .catch(function(error) {
          if (error.statusCode === 404) {
            callback(null, false, false);
          } else {
            callback(error);
          }
        });
    },

    createBlockBlobFromLocalFile: function(
      containerName,
      blobName,
      localFilePath,
      options,
      callback
    ) {
      var containerURL = ContainerURL.fromServiceURL(serviceURL, containerName);
      var blobURL = BlobURL.fromContainerURL(containerURL, blobName);
      var blockBlobURL = BlockBlobURL.fromBlobURL(blobURL);
      uploadFileToBlockBlob(
        Aborter.none,
        localFilePath,
        blockBlobURL,
        Object.assign(
          {
            blockSize: 4 * 1024 * 1024, // 4MB block size
            parallelism: 20 // 20 concurrency
          },
          options
        )
      )
        .then(function(response) {
          callback(null, blockBlobURL.url, response);
        })
        .catch(function(error) {
          callback(error, null);
        });
    }
  };
};
