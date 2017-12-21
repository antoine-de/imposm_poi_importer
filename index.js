const { Pool } = require('pg');
const _ = require('lodash');
const { Document } = require('pelias-model');
const http = require('tiny-json-http');
const elasticsearch = require('elasticsearch');
const yargs = require('yargs');
const QueryStream = require('pg-query-stream');

const { Writable } = require('stream');

const LAYER = 'venue';
const SOURCE = 'openstreetmap';
const CURSOR_SIZE = 100;


async function getAllPois(pg, cb) {
  const query = `select id,
    st_x(st_transform(geometry, 4326)) as lon,
    st_y(st_transform(geometry, 4326)) as lat,
    name
    from osm_poi_point where name <> '' limit 100`;

  const pool = new Pool({
    connectionString: pg,
  });
  const client = await pool.connect();

  console.log('querying poi database');
  const queryStream = new QueryStream(query, [], { batchSize: CURSOR_SIZE });
  const stream = client.query(queryStream);
  // release the client when the stream is finished
  stream.on('end', () => { pool.end(); });
  stream.on('data', (row) => {
    cb(row);
  });
}

function createPeliasDocument(p) {
  const doc = new Document(SOURCE, LAYER, p.id);

  doc.setName('default', _.trim(p.name));
  doc.setCentroid({
    lat: p.lat,
    lon: p.lon,
  });
  if (!_.isEmpty(_.trim(p.house_number))) {
    doc.setAddress('number', _.trim(p.house_number));
  }
  if (!_.isEmpty(_.trim(p.street))) {
    doc.setAddress('street', _.trim(p.street));
  }
  if (!_.isEmpty(_.trim(p.postcode))) {
    doc.setAddress('zip', _.trim(p.postcode));
  }

  return doc;
}

async function locate(peliasPoi, pip) {
  const url = `${pip}/${peliasPoi.center_point.lon}/${peliasPoi.center_point.lat}`;
  try {
    const res = await http.get({ url });

    Object.entries(res.body).forEach(([layer, admin]) => {
      peliasPoi.addParent(
        layer,
        admin[0].name,
        admin[0].id.toString(),
        admin[0].abbr,
      );
    });
  } catch (e) {
    setImmediate(() => { throw e; });
  }
  return peliasPoi;
}

function convertToPelias(pois, pip) {
  return pois.map((poi) => {
    const doc = createPeliasDocument(poi);
    return locate(doc, pip);
  });
}

function sendToEs(peliasPois, esHost) {
  const client = new elasticsearch.Client({
    host: esHost,
    log: 'info',
  });

  let nbErrors = 0;
  let nbAttempt = 0;

  peliasPois.forEach((poi) => {
    nbAttempt += 1;
    poi.then(p => p.toESDocument().data)
      .then(async (doc) => {
        console.log('document send to es: ', JSON.stringify(doc, null, 4));
        return client.index({
          index: 'pelias',
          type: 'venue',
          id: `imposm:${doc.source_id}`,
          body: doc,
        });
      })
      .catch((e) => {
        console.error('impossible to insert doc ', poi, 'because ', e);
        nbErrors += 1;
      });

    if (nbErrors > 0) {
      console.error(`${nbErrors} errors /${nbAttempt} documents`);
    }
  });
}

function importPoiInPelias(pg, es, pip) {
  getAllPois(pg, (chunk) => {
    console.log('chunk ', chunk);
    const peliasPois = convertToPelias([chunk], pip);
    sendToEs(peliasPois, es);
  });
  // getAllPois(pg)
  //   .then(pois => convertToPelias(pois, pip))
  //   .then(peliasPois => sendToEs(peliasPois, es))
  //   .catch(e => setImmediate(() => { throw e; }));
}

const args = yargs
  .usage('Usage: $0 [options]')
  .options({
    pg: {
      describe: 'the connection-string to postgres',
      type: 'string',
      demandOption: true,
      default: 'postgres://gis:gis@localhost/gis',
      nargs: 1,
    },
  })
  .options({
    es: {
      describe: 'The host of ElasticSearch',
      type: 'string',
      demandOption: true,
      default: 'localhost:9200',
      nargs: 1,
    },
  })
  .options({
    pip: {
      describe: 'The url of the Point In Polygon service',
      type: 'string',
      demandOption: true,
      default: 'http://localhost:4200',
      nargs: 1,
    },
  })
  .help('h')
  .alias('h', 'help')
  .argv;

importPoiInPelias(args.pg, args.es, args.pip);
