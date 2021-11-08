from typing import Union, Tuple
import uuid
from typing import Optional
from datetime import datetime, timezone
from dataclasses import dataclass
import psycopg2
from psycopg2.extensions import connection, cursor
from psycopg2.extras import DictCursor


@dataclass
class FilmWork:
    fw_id: uuid
    title: str
    type: str
    created_at: datetime
    updated_at: datetime
    p_id: uuid
    full_name: Optional[str] = None
    name: Optional[str] = None
    role: Optional[str] = None
    rating: Optional[float] = None
    description: Optional[str] = None


class Extract:
    def __init__(self, dsl, limit):
        self.dsl = dsl
        self.limit = limit

    def _con(self) -> connection:
        return psycopg2.connect(**self.dsl, cursor_factory=DictCursor)

    def _cur(self) -> cursor:
        return self._con().cursor()

    def fetch_data(self, last_update, limit) -> Tuple[list, Optional[datetime]]:
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
                       g.name
                   FROM content.film_work fw
                   LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                   LEFT JOIN content.person p ON p.id = pfw.person_id
                   LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                   LEFT JOIN content.genre g ON g.id = gfw.genre_id
                   WHERE fw.updated_at > %(updated_at)s
                   GROUP BY fw.id, pfw.role, p.id, g.name
                   ORDER BY fw.updated_at ASC
                   LIMIT %(limit)s;
               """, {'updated_at': last_update, 'limit': limit})
        fetch_data = cur.fetchall()






        #
        # cur.execute("""
        #     SELECT id, updated_at
        #     FROM content.person
        #     WHERE updated_at > %s
        #     ORDER BY updated_at
        #     LIMIT %s;
        # """, (kwargs['update_at'], self.limit))
        # person_ids = tuple(dict(row).get('id') for row in cur.fetchall())
        #
        # cur.execute("""
        #     SELECT fw.id, fw.updated_at
        #     FROM content.film_work fw
        #     LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
        #     WHERE pfw.person_id IN %s
        #     ORDER BY fw.updated_at;
        # """, (person_ids, ))
        # fw_ids = tuple(dict(row).get('id') for row in cur.fetchall())
        #
        # cur.execute("""
        #     SELECT
        #         fw.id as fw_id,
        #         fw.title,
        #         fw.description,
        #         fw.rating,
        #         fw.type,
        #         fw.created_at,
        #         fw.updated_at,
        #         pfw.role,
        #         p.id as p_id,
        #         p.full_name,
        #         g.name
        #     FROM content.film_work fw
        #     INNER JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
        #     INNER JOIN content.person p ON p.id = pfw.person_id
        #     INNER JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
        #     INNER JOIN content.genre g ON g.id = gfw.genre_id
        #     WHERE fw.id IN %s ORDER BY fw.title;
        # """, (fw_ids, ))
        # fetch_data = cur.fetchall()
        data = [FilmWork(**dict(row)) for row in fetch_data]
        if len(data) == 0:
            return [], None
        return data, data[-1].updated_at
