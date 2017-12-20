'use strict'

const { Pool } = require('pg');
const _ = require('lodash');
const Document = require('pelias-model').Document;
const http = require('tiny-json-http');
const elasticsearch = require('elasticsearch');
const yargs = require('yargs');

const LAYER = 'venue';
const SOURCE = 'openstreetmap';

async function get_all_pois(pg) {
  const query = `select id,
    st_x(st_transform(geometry, 4326)) as lon,
    st_y(st_transform(geometry, 4326)) as lat,
    name
    from osm_poi_point where name <> '' limit 100`;

  const pool = new Pool({
    connectionString: pg,
  })

  console.log(`querying poi database`);
  const res = await pool.query(query);

  await pool.end();
  return res.rows;
}

function create_pelias_document(p) {
  const doc = new Document(SOURCE, LAYER, p.id);

  doc.setName('default', _.trim(p.name));
  doc.setCentroid({
    lat: p.lat,
    lon: p.lon
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

async function locate(pelias_poi, pip) {
  const url = `${pip}/${pelias_poi.center_point.lon}/${pelias_poi.center_point.lat}`;
  try {
    const res = await http.get({ url });

    Object.entries(res.body).forEach(([layer, admin]) => {
      pelias_poi.addParent(
        layer,
        admin[0].name,
        admin[0].id.toString(),
        admin[0].abbr);
    });
    return pelias_poi;
  } catch (e) {
    setImmediate(() => { throw e });
  }
}

function convert_to_pelias(pois, pip) {
  return pois.map(poi => {
    const doc = create_pelias_document(poi);
    return locate(doc, pip);
  });
}

function send_to_es(pelias_pois, es_host) {
  const client = new elasticsearch.Client({
    host: es_host,
    log: 'info'
  });

  var nb_errors = 0,
    nb_attempt = 0;

  pelias_pois.forEach(poi => {
    nb_attempt++;
    poi.then(p => p.toESDocument().data)
      .then(async doc => {
        console.log("document send to es: ", JSON.stringify(doc, null, 4));
        return client.index({
          index: 'pelias',
          type: 'venue',
          id: 'imposm:' + doc.source_id,
          body: doc
        });
      })
      .catch(e => {
        console.error("impossible to insert doc ", poi, "because ", e);
        nb_errors++;
      });

    if (nb_errors > 0) {
      console.error(`${nb_errors} errors /${nb_attempt} documents`)
    }
  });
}

function import_poi_in_pelias(pg, es, pip) {
  get_all_pois(pg)
    .then(pois => convert_to_pelias(pois, pip))
    .then(pelias_pois => send_to_es(pelias_pois, es))
    .catch(e => setImmediate(() => { throw e }))
}

let args = yargs
  .usage('Usage: $0 [options]')
  .options({
    pg: {
      describe: 'the connection-string to postgres',
      type: 'string',
      demandOption: true,
      default: 'postgres://gis:gis@localhost/gis',
      nargs: 1
    }
  })
  .options({
    es: {
      describe: 'The host of ElasticSearch',
      type: 'string',
      demandOption: true,
      default: 'localhost:9200',
      nargs: 1
    }
  })
  .options({
    pip: {
      describe: 'The url of the Point In Polygon service',
      type: 'string',
      demandOption: true,
      default: 'http://localhost:4200',
      nargs: 1
    }
  })
  .help('h')
  .alias('h', 'help')
  .argv;

import_poi_in_pelias(args.pg, args.es, args.pip);
