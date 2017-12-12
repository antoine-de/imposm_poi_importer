# imposm_poi_importer
import all pois imported by imposm in a postgresql database to pelias

it needs a running https://github.com/pelias/document-service

## install
`pipenv install`

## run
`pipenv run import.py`

To get the updated list of parameters:
`pipenv run import.py -- --help`

parameters:
 * pg: postgres connection string
 * es: elastic search connection string
 * ps: document service url
