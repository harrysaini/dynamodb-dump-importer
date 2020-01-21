const fs = require('fs');
const path = require('path');
const csvToJson = require('./externals/csvParser.js');
const DB = require('./dbWriter');

const REGEX = {
  typeInHeader: new RegExp('(\\(.*\\))', 'gi')
}

const parseCSVToJSON = csv => csvToJson(csv, {
  parseJSON: true,
  parseNumbers: true,
});

const getFieldNameAndTypeFromKey = key => {
  const matches = key.match(REGEX.typeInHeader);
  const typeMatch = matches[0];
  const type = typeMatch.replace('(', '').replace(')', '');
  const name = key.replace(typeMatch, '').trim();
  // console.log(type, name);
  return {
    type,
    name
  }
}

const valParsers = {
  'S': val => String(val),
  'N': val => String(parseFloat(val))
}

const parseValue = (val, type) => {
  return valParsers[type] ? valParsers[type](val) : val ;
}


const createDBItemsObjects = (objs) => {
  const dbWrites = objs.map(obj => {
    const writeObj = {};
    const keys = Object.keys(obj);
    keys.forEach(key => {
      const { type, name } = getFieldNameAndTypeFromKey(key);
      writeObj[name] = {
        [type]: parseValue(obj[key], type)
      }
    });
    return writeObj;
  });
  return dbWrites;
}

const parseAndWriteSingleFileDump = async (csvFile) => {
  const file = fs.readFileSync(csvFile, 'utf-8');

  const pathArr = csvFile.split('/');
  const filename = pathArr.splice(-1)[0];
  const dir = pathArr.join('/');

  const objs = parseCSVToJSON(file);
  const tableName = filename.replace('.csv', '');
  const dbWrites = createDBItemsObjects(objs);
  await DB.writeItemsInBatch(dbWrites, tableName);
}

/**
 * START CODE EXECUTION FROM HERE
 */
const start = async () => {
  const tables = await DB.listTables();
  console.log(tables);
  const dir = process.argv[2];
  const files = fs.readdirSync(dir);
  console.log(files);
  for(let file of files){
    console.log(`Writing dump for -> ${file}`)
    const filePath = path.join(dir, file);
    await parseAndWriteSingleFileDump(filePath);
    console.log('Sucess !!!');
  };
}

start()
  .then(data => {
    console.log('Data added succesfully');
  })
  .catch((err) => {
    console.log(err);
  });
