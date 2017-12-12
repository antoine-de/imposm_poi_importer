#!/usr/bin/env python
import fire
import records
import requests
from elasticsearch import Elasticsearch
from elasticsearch import TransportError
import logging


def send_to_es(id, poi, es):
    try:
        esResponse = es.index(index='pelias', doc_type='venue', id=id, body=poi)
        logging.debug(f'es resp = {esResponse}')
    except TransportError as err:
        logging.exception(f'error while loading in es {err}')


def get_all_pois(pg_cnx):
    pg = records.Database(pg_cnx)
    pois = pg.query("select id, "
                    "st_x(st_transform(geometry, 4326)) as lon, "
                    "st_y(st_transform(geometry, 4326)) as lat, "
                    "name "
                    "from osm_poi_point where name <> '';")

    for poi in pois:
        yield poi


def transform_to_pelias(poi, ds_url):
    resp = requests.get(ds_url + '/openstreetmap/venue', params=poi.as_dict())

    if resp.status_code != 200:
        logging.error(f'ERROR: {resp}')
        return

    pelias_poi = resp.json()
    logging.debug(f'pelias poi = {pelias_poi}')
    return pelias_poi


def import_to_pelias(pg='postgres://gis:gis@localhost/gis',
                     es='localhost:9200',
                     ds='http://localhost:5000/synthesize'):
    """
    import all pois imported by imposm in a postgresql database to pelias
    :param pg: postgresql connection string
    :param es: elastic search url
    :param ds: pelias document service url
    """
    es = Elasticsearch([es])

    for poi in get_all_pois(pg):
        logging.info(f'poi = {poi}')

        pelias_poi = transform_to_pelias(poi, ds)

        send_to_es(poi.id, pelias_poi, es)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    fire.Fire(import_to_pelias)
