import fire
import records
import requests
from elasticsearch import Elasticsearch
from elasticsearch import TransportError


def send_to_es(id, poi, es):
    try:
        esResponse = es.index(index='pelias', doc_type='venue', id=id, body=poi)
        print(f'es resp = {esResponse}')
    except TransportError as err:
        print(f'error while loading in es {err}')


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
        print(f'ERROR: {resp}')
        return

    pelias_poi = resp.json()
    print(f'pelias poi = {pelias_poi}')
    return pelias_poi


def import_to_pelias(pg='postgres://gis:gis@localhost/gis',
                     es='localhost:9200',
                     ds_url='http://localhost:5000/synthesize'):
    es = Elasticsearch([es])

    for poi in get_all_pois(pg):
        print(f'poi = {poi}')

        pelias_poi = transform_to_pelias(poi, ds_url)

        send_to_es(poi.id, pelias_poi, es)


if __name__ == '__main__':
    fire.Fire(import_to_pelias)
