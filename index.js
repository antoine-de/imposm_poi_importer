const { Pool } = require('pg');
const _ = require('lodash');
const { Document } = require('pelias-model');
const http = require('tiny-json-http');
const elasticsearch = require('elasticsearch');
const yargs = require('yargs');
const Cursor = require('pg-cursor');

const LAYER = 'venue';
const SOURCE = 'openstreetmap';
const CURSOR_SIZE = 100;

/*
 * processAllPois query all the database pois with chunk and call
 * the callback `cb` on those chunks
 */
async function processAllPois(pg, cb) {
  const query = `select id,
    st_x(st_transform(geometry, 4326)) as lon,
    st_y(st_transform(geometry, 4326)) as lat,
    name
    from osm_poi_point where name <> ''`;

  const pool = new Pool({
    connectionString: pg,
  });
  const client = await pool.connect();
  console.log('querying poi database');
  // we use cursor for efficient query pagination
  const cursor = client.query(new Cursor(query));

  let nbDone = 0;
  const job = async (err, ch) => {
    if (err) {
      throw err;
    }
    console.log('nb elt: ', ch.length);
    if (ch.length === 0) {
      console.log('closing connection');
      await client.end();
      console.log('connection closed');
      return;
    }
    nbDone += ch.length;
    console.log(`${nbDone} elements proceced`);
    cb(ch); // call the callback on the rows
    cursor.read(CURSOR_SIZE, job); // read the next chunk
  };
  cursor.read(CURSOR_SIZE, job);
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

/*
 * Call Point In Polygon to query the Who's On First hierarchy for the POI
 */
async function locate(peliasPoi, pip) {
  const url = `${pip}/${peliasPoi.center_point.lon}/${peliasPoi.center_point.lat}`;
  try {
    const res = await http.get({ url });
    _.forEach(res.body, ([{ name, id, abbr }], layer) => {
      peliasPoi.addParent(
        layer,
        name,
        id.toString(),
        abbr,
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

async function sendToEs(peliasPois, esClient) {
  const createAction = doc => ({
    index: {
      _index: 'pelias',
      _type: 'venue',
      _id: `imposm:${doc.source_id}`,
    },
  });

  const actionsPromises = peliasPois.map(poi =>
    poi
      .then(p => p.toESDocument().data)
      .then(doc => [createAction(doc), doc]));

  try {
    const actions = await Promise.all(actionsPromises);
    const bulkActions = _.flatten(actions);
    // bulkActions.forEach(a => console.log(JSON.stringify(a)));
    esClient.bulk({
      body: bulkActions,
    }).catch((err) => {
      setImmediate(() => { throw err; });
      throw err;
    });
  } catch (err) {
    setImmediate(() => { throw err; });
  }
}

function importPoiInPelias(pg, es, pip) {
  const esClient = new elasticsearch.Client({
    host: es,
    log: 'info',
  });

  processAllPois(pg, (chunk) => {
    const peliasPois = convertToPelias(chunk, pip);
    sendToEs(peliasPois, esClient);
  });
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
