from typing import Tuple
from typing import Optional
from datetime import datetime
from dataclasses import dataclass
from uuid import UUID

import psycopg2
from psycopg2.extensions import connection, cursor
from psycopg2 import OperationalError
from psycopg2.extras import DictCursor
from misc import backoff


@dataclass
class FilmWork:
    fw_id: UUID
    title: str
    type: str
    created_at: datetime
    updated_at: datetime
    p_id: UUID
    greatest: datetime
    full_name: Optional[str] = None
    name: Optional[str] = None
    role: Optional[str] = None
    rating: Optional[float] = None
    description: Optional[str] = None


class Extract:
    """
    The class implements unloading data from Postgres database
    """
    def __init__(self, dsl: dict, limit: int):
        self.dsl = dsl
        self.limit = limit

    @backoff(OperationalError)
    def _con(self) -> connection:
        """
        Set connection to db
        """
        con = psycopg2.connect(**self.dsl, cursor_factory=DictCursor)
        return con

    def _cur(self) -> cursor:
        """
        Get cursor
        """
        con = self._con()
        return con.cursor()

    def fetch_data(self, last_update: datetime, limit: int) -> Tuple[list, Optional[datetime]]:
        """
        Fetch data from database
        """
        cur = self._cur()

        cur.execute("""
                   SELECT
                       fw.id as fw_id, 
                       fw.title, 
                       fw.description, 
                       fw.rating, 
                       fw.type, 
                       fw.created_at, 
                       fw.updated_at, 
                       pfw.role, 
                       p.id as p_id, 
                       p.full_name,
                       g.name,
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
