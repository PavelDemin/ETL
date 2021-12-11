import datetime
import itertools
import dataclasses
import logging
from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel

from models import FilmWorkIndex



logger = logging.getLogger()


class ElPerson(BaseModel):
    id: UUID
    name: str


class ElFilmWork(BaseModel):
    id: UUID
    imdb_rating: Optional[float] = None
    genre: List[str]
    title: str
    description: Optional[str] = None
    director: List[str]
    actors_names: List[str]
    writers_names: List[str]
    actors: List[ElPerson]
    writers: List[ElPerson]


class Transform:
    """
    The class implements transform data to load in Elasticsearch database
    """
    def __init__(self, data: List[FilmWorkIndex]):
        self.data = data

    def get_data(self) -> List[dict]:
        """ The method returns a generator with prepared data """
        for item in self.data:
            action = {
                "_index": "movies",
                "_id": item.uuid,
                "_source": dataclasses.asdict(item)
            }
            yield action
