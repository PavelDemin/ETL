from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Tuple
from uuid import UUID

import psycopg2
from misc import backoff
from psycopg2 import OperationalError
from psycopg2.extensions import connection
from psycopg2.extras import DictCursor


@dataclass
class FilmWork:
    id: UUID
    title: str
    type: str
    created_at: datetime
    updated_at: datetime
    person_id: UUID
    greatest: datetime
    person_full_name: Optional[str] = None
    genre: Optional[str] = None
    person_role: Optional[str] = None
    rating: Optional[float] = None
    description: Optional[str] = None


class Extract:
    """
    The class implements unloading data from Postgres database
    """

    def __init__(self, dsl: dict, limit: int):
        self.dsl = dsl
        self.limit = limit
        self._con: Optional[connection] = None

    @backoff(OperationalError)
    def create_connection(self):
        """
        Create connection to db
        """
        return psycopg2.connect(**self.dsl, cursor_factory=DictCursor)

    @property
    def con(self) -> connection:
        """
        Return pg connection
        """
        if self._con is None or self._con.closed:
            self._con = self.create_connection()
        return self._con

    @backoff(OperationalError)
    def fetch_data(self, last_update: datetime, limit: int) -> Tuple[list, Optional[datetime]]:
        """
        Fetch data from database
        """
        with self.con.cursor() as cur:
            cur.execute("""
                       SELECT
                           fw.id as id, 
                           fw.title as title, 
                           fw.description as description, 
                           fw.rating as rating, 
                           fw.type as type, 
                           fw.created_at as created_at, 
                           fw.updated_at as updated_at, 
                           pfw.role as person_role, 
                           p.id as person_id, 
                           p.full_name as person_full_name,
                           g.name as genre,
                           Greatest(fw.updated_at, p.updated_at, g.updated_at) as greatest
                       FROM film_work fw
                       LEFT JOIN person_film_work pfw ON pfw.film_work_id = fw.id 
                       LEFT JOIN person p ON p.id = pfw.person_id
                       LEFT JOIN genre_film_work gfw ON gfw.film_work_id = fw.id 
                       LEFT JOIN genre g ON g.id = gfw.genre_id
                       WHERE fw.updated_at > %(updated_at)s OR p.updated_at > %(updated_at)s OR g.updated_at > %(updated_at)s
                       ORDER BY Greatest(fw.updated_at, p.updated_at, g.updated_at) ASC
                       LIMIT %(limit)s;
                   """, {'updated_at': last_update, 'limit': limit})
            fetch_data = cur.fetchall()
        data = [FilmWork(**dict(row)) for row in fetch_data]
        if len(data) == 0:
            return [], None
        return data, data[-1].greatest
