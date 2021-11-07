from typing import Union
from uuid import uuid4
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch.exceptions import NotFoundError


class Load:
    def __init__(self):
        self.es = Elasticsearch()

    def crate_index(self, index, body):
        return self.es.indices.create(index=index, body=body)

    def load_data(self, data):
        return bulk(self.es, data)

    def cat_index(self, index):
        try:
            return self.es.cat.indices(index='movies')
        except NotFoundError:
            return False


    def get_index(self, index: Union[list[str], str]):
        return self.es.indices.get(index=index)

