import datetime
import itertools
import logging
from typing import Optional
from uuid import UUID

from pydantic import BaseModel

from extract import FilmWork

logger = logging.getLogger()


class ElPerson(BaseModel):
    id: UUID
    name: str


class ElFilmWork(BaseModel):
    id: UUID
    imdb_rating: Optional[float] = None
    genre: list[str]
    title: str
    description: Optional[str] = None
    director: list[str]
    actors_names: list[str]
    writers_names: list[str]
    actors: list[ElPerson]
    writers: list[ElPerson]


class Transform:
    """
    The class implements transform data to load in Elasticsearch database
    """
    def __init__(self, data: list[FilmWork]):
        self.data = data

    def _transform_data(self) -> list[ElFilmWork]:
        """ This method transform data"""
        last_date = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
        for key, group in itertools.groupby(self.data, lambda x: x.id):
            actors = []
            writers = []
            directors = []
            genres = []
            actors_names = []
            writers_names = []
            for fw_row in group:
                if fw_row.person_id is not None:
                    person = ElPerson(id=fw_row.person_id, name=fw_row.person_full_name)
                    if fw_row.person_role == 'actor' and person not in actors:
                        actors.append(person)
                        actors_names.append(fw_row.person_full_name)
                    if fw_row.person_role == 'writer' and person not in writers:
                        writers.append(person)
                        writers_names.append(fw_row.person_full_name)
                    if fw_row.person_role == 'director' and fw_row.person_full_name not in directors:
                        directors.append(fw_row.person_full_name)
                if fw_row.genre not in genres:
                    genres.append(fw_row.genre)
                if fw_row.updated_at > last_date:
                    last_date = fw_row.updated_at
            cls = ElFilmWork(
                id=fw_row.id,
                imdb_rating=fw_row.rating,
                genre=genres,
                title=fw_row.title,
                description=fw_row.description,
                director=directors,
                actors_names=actors_names,
                writers_names=writers_names,
                actors=actors,
                writers=writers
            )
            logger.debug("Added FilmWork %s", (cls,))
            yield cls

    def get_data(self):
        """ The method returns a generator with prepared data """
        for row in self._transform_data():
            doc = {
                "_index": "movies",
                "_id": str(row.id),
                "id": str(row.id),
                "imdb_rating": row.imdb_rating,
                "genre": [", ".join(row.genre)],
                "title": row.title,
                "description": row.description,
                "director": row.director if len(row.director) != 0 else None,
                "actors_names": row.actors_names,
                "writers_names": row.writers_names,
                "actors": [i.dict() for i in row.actors],
                "writers": [i.dict() for i in row.writers]
            }
            yield doc
