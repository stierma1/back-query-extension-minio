
var MinioClient = require("minio").Client;
var {gzip, ungzip} = require("node-gzip");

module.exports = function(base){
  return class QueryManagerExtension extends base{
    constructor(connectionInfo){
      super(connectionInfo);
      var {Endpoint, Port, UseSSL, AccessKey, SecretKey} = this.run(`minio_endpoint(Endpoint),
        minio_port(Port),
        minio_usessl(UseSSL),
        minio_accesskey(AccessKey),
        minio_secretkey(SecretKey)
      `).first();

      this.minioClient = new MinioClient({
        endPoint:Endpoint,
        port:Port,
        useSSL: UseSSL === "true",
        accessKey: AccessKey,
        secretKey: SecretKey
      });
    }

    async uploadToMinio(bucket, objectName, obj, compress){
      var gzipped;
      if(compress){
        gzipped = await gzip(obj);
      } else {
        if(obj instanceof Buffer){
          gzipped = obj;
        } else {
          gzipped = Buffer.from(obj, "utf8");
        }
      }

      return new Promise((res, rej) => {
        this.minioClient.putObject(bucket, objectName, gzipped, (err) => {
          if(err){
            return rej(err);
          }
          res();
        });
      })
    }

    listObjectsFromMinio(bucket, prefix, recursive){
      return new Promise((res, rej) => {
        var stream = this.minioClient.listObjects(bucket, prefix, recursive);
        var objs = [];
        stream.on("data", (d) => {
          objs.push(d);
        });
        stream.on("error", (e) => {
          rej(e);
        });
        stream.on("end", () => {
          res(objs);
        });
      })
    }

    async getObjectFromMinio(bucket, objectName, decompress){
      var prom = new Promise((res, rej) => {
       this.minioClient.getObject(bucket, objectName, (err, dataStream) => {
         if(err){
           return rej(err);
         }

         var data = new Buffer(0);
         dataStream.on('data', function(chunk) {
           data = Buffer.concat([data,chunk], data.length + chunk.length);
         });
         dataStream.on('end', function() {
           res(data);
         });
         dataStream.on('error', function(err) {
           rej(err);
         });
       });
     });

     var gqString = await prom;
     var qString;
     if(decompress){
       qString = await ungzip(gqString);
     } else {
       qString = gqString;
     }
     return qString;
    }

    removeObjectsFromMinio(bucket, objectNames){
      return new Promise((res, rej) => {
       this.minioClient.removeObjects(bucket, objectNames, (err) => {
         if(err){
           return rej(err);
         }
         res();
       });
     });
    }

    async loadFromMinio(bucket, objectName, decompress){
       var prom = new Promise((res, rej) => {
        this.minioClient.getObject(bucket, objectName, (err, dataStream) => {
          if(err){
            return rej(err);
          }

          var data = new Buffer(0);
          dataStream.on('data', function(chunk) {
            data = Buffer.concat([data,chunk], data.length + chunk.length);
          });
          dataStream.on('end', function() {
            res(data);
          });
          dataStream.on('error', function(err) {
            rej(err);
          });
        });
      });

      var gqString = await prom;
      var qString;
      if(decompress){
        qString = await ungzip(gqString);
      } else {
        qString = gqString.toString("utf8");
      }

      return super.loadQueryString(qString.toString());
    }

    async runFromMinio(bucket, objectName, decompress){
      var prom = new Promise((res, rej) => {
       this.minioClient.getObject(bucket, objectName, (err, dataStream) => {
         if(err){
           return rej(err);
         }

         var data = new Buffer(0);
         dataStream.on('data', function(chunk) {
           data = Buffer.concat([data,chunk], data.length + chunk.length);
         });
         dataStream.on('end', function() {
           res(data);
         });
         dataStream.on('error', function(err) {
           rej(err);
         });
       });
     });
     var gqString = await prom;
     var qString
     if(decompress){
       qString = await ungzip(gqString);
     } else{
       qString = gqString.toString("utf8");
     }

     return super.run(qString.toString());
    }

  }

}
