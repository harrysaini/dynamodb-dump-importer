const CHUNK_SIZE = 20;
var AWS = require("aws-sdk");

AWS.config.update({
  region: "eu-west-1",
  endpoint: "http://localhost:8000",
  accessKeyId: "DummyKeyForLocalDB",
  secretAccessKey: "DummyKeySecretForLocalDB"
});

const apiVersion = { apiVersion: '2012-08-10' }

// const docClient = new AWS.DynamoDB.DocumentClient(apiVersion);
const dbClient = new AWS.DynamoDB(apiVersion);


const chunkifyArray = (arr, chunkSize) => {
  const res = [];
  let counter = 0;
  while (counter < arr.length) {
    res.push(arr.slice(counter, counter + chunkSize));
    counter = counter + chunkSize;
  }
  return res;
}

writeSingleBatch = (items, tableName) => {
  const writeObj = {
    RequestItems: {
      [tableName]: items.map(item => {
        return {
          PutRequest: {
            Item: item
          }
        };
      })
    }
  }

  console.log(JSON.stringify(writeObj));
  return dbClient.batchWriteItem(writeObj).promise();
}

exports.writeItemsInBatch = async (items, tableName)  => {
  // cleconsole.log(`Writing to table ${tableName} -> ${JSON.stringify(items)}`);
  const chunks = chunkifyArray(items, CHUNK_SIZE);
  const writePromises = chunks.map(chunk => writeSingleBatch(chunk, tableName));
  const res = await Promise.all(writePromises);
  return res;
}

exports.listTables = async () => {
  return dbClient.listTables().promise();
}

