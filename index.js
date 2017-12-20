'use strict'

const { Pool } = require('pg');
const _ = require('lodash');
const Document = require('pelias-model').Document;
const http = require('tiny-json-http');
const elasticsearch = require('elasticsearch');

const LAYER = 'venue';
const SOURCE = 'openstreetmap';

async function get_all_pois() {
  const query = `select id, 
    st_x(st_transform(geometry, 4326)) as lon, 
    st_y(st_transform(geometry, 4326)) as lat, 
    name 
    from osm_poi_point where name <> '' limit 10;`;

  const pool = new Pool()

  const res = await pool.query(query);

  console.log('poi:', res.rows[0]);
  console.log('poi:', res.rows[0].id);

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

async function locate(pelias_poi) {
  console.log(`pois ${pelias_poi}`);
  const url = `http://localhost:4200/${pelias_poi.center_point.lon}/${pelias_poi.center_point.lat}`;
  console.log(`curl  ${url}`);
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
    console.log(`damn une erreur ${e}`, e)
  }
}

function convert_to_pelias(pois) {
  console.log(`pois ${pois}`);


  return pois.map(poi => {
    const doc = create_pelias_document(poi);
    return locate(doc);
  });
}

function send_to_es(pelias_pois) {
  pelias_pois.forEach(poi => {
    poi.then(p => p.toESDocument().data)
      .then(async doc => {
        console.log(JSON.stringify(doc, null, 4));
        const client = new elasticsearch.Client({
          host: 'localhost:9200',
          log: 'info'
        });

        return client.create({
          index: 'pelias',
          type: 'venue',
          id: 'imposm:' + doc.source_id,
          body: doc
        });
      })
      .catch(e => console.error("impossible to insert doc ", poi, "because ", e));

  });
}

function import_poi_in_pelias() {
  get_all_pois()
    .then(pois => convert_to_pelias(pois))
    .then(pelias_pois => send_to_es(pelias_pois))
    .catch(e => setImmediate(() => { throw e }))
}

import_poi_in_pelias();