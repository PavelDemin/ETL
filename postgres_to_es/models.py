import uuid
from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class PersonRow():
    uuid: uuid.UUID
    full_name: str

class GenreRow():
    uuid: uuid.UUID
    name: str

@dataclass
class FilmWorkIndex():
    uuid: uuid.UUID
    imdb_rating: float
    genres_names: str
    genre: List[GenreRow]
    title: str
    description: str
    director: Optional[str]
    actors_names: Optional[str]
    writers_names: Optional[str]
    actors: List[PersonRow]
    writers: List[PersonRow]
    directors: List[PersonRow]
