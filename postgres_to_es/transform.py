import datetime
from typing import Optional
from extract import FilmWork
import itertools
from pydantic import BaseModel
from uuid import UUID
import misc
import logging


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
    def __init__(self, data: list[FilmWork]):
        self.data = data

    def _transform_data(self):
        fw = []
        last_date = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
        for key, group in itertools.groupby(self.data, lambda x: x.fw_id):
            actors = []
            writers = []
            directors = []
            genres = []
            actors_names = []
            writers_names = []
            group_list = list(group)
            for i in group_list:
                if i.p_id is not None:
                    person = ElPerson(id=i.p_id, name=i.full_name)
                    if i.role == 'actor' and person not in actors:
                        actors.append(person)
                        actors_names.append(i.full_name)
                    if i.role == 'writer' and person not in writers:
                        writers.append(person)
                        writers_names.append(i.full_name)
                    if i.role == 'director' and i.full_name not in directors:
                        directors.append(i.full_name)
                if i.name not in genres:
                    genres.append(i.name)
                if last_date is None or i.updated_at > last_date:
                    last_date = i.updated_at
                if i == group_list[-1]:
                    cls = ElFilmWork(
                        id=i.fw_id,
                        imdb_rating=i.rating,
                        genre=genres,
                        title=i.title,
                        description=i.description,
                        director=directors,
                        actors_names=actors_names,
                        writers_names=writers_names,
                        actors=actors,
                        writers=writers
                    )
                    fw.append(cls)
                    logging.debug("Added FilmWork %s", (cls, ))

        return fw

    def get_data(self):
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
                "actors": [dict(i) for i in row.actors],
                "writers": [dict(i) for i in row.writers]
            }
            yield doc
