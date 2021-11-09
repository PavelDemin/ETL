from typing import Union
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch.exceptions import NotFoundError, ConnectionError
from misc import backoff


class Load:
    def __init__(self, dsl):
        self.dsl = dsl

    @backoff(ConnectionError)
    def _es(self):
        es = Elasticsearch([self.dsl])
        es.info()
        return es

    def crate_index(self, index, body):
        return self._es().indices.create(index=index, body=body)

    def load_data(self, data):
        return bulk(self._es(), data)

    def cat_index(self, index):
        try:
            return self._es().cat.indices(index)
        except NotFoundError:
            return False

    def get_index(self, index: Union[list[str], str]):
        return self._es().indices.get(index=index)

