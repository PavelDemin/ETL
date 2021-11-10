from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch.exceptions import NotFoundError, ConnectionError
from misc import backoff


class Load:
    """
    The class implements downloading data to Elasticsearch database
    """
    def __init__(self, dsl: dict):
        self.dsl = dsl

    @backoff(ConnectionError)
    def _es(self):
        """
        Create ElasticSearch client
        """
        es = Elasticsearch([self.dsl])
        es.info()
        return es

    def crate_index(self, index: str, body: dict):
        """
        Create index database
        """
        return self._es().indices.create(index=index, body=body)

    def load_data(self, data: list):
        """
        Load data to index
        """
        return bulk(self._es(), data)

    def cat_index(self, index: str):
        """
        Check index is exist
        """
        try:
            return self._es().cat.indices(index=index)
        except NotFoundError:
            return False

