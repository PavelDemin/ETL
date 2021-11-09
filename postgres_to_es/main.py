import json
from time import sleep
from load import Load
import misc
from transform import Transform
from extract import Extract
from config import config
from state import State, JsonFileStorage
import logging

pg_dsl = {
    "dbname": config.PG_DBNAME,
    "user": config.PG_USER,
    "password": config.PG_PASSWORD.get_secret_value(),
    "host": config.PG_HOST,
    "port": config.PG_PORT
}
el_dsl = {'host': config.ES_HOST, "port": config.ES_PORT}

extract = Extract(pg_dsl, config.LIMIT)
es_load = Load(el_dsl)

state = State(JsonFileStorage(config.STATE_FILE_PATH))


def create_indices():
    with open(config.INDICES_FILE_PATH, "r") as file:
        indices: dict = json.load(file)
        for index in indices.keys():
            if es_load.cat_index(index) is False:
                logging.info(f"Index {index} not found!")
                es_load.crate_index(index, indices[index])
                logging.info("Index {index} create successful.")


def main():
    create_indices()
    while True:
        logging.info(f"Start extract data from Postgres server with limit {config.LIMIT}.")
        st = state.get_state('updated_at')
        data, updated_at = extract.fetch_data(st, config.LIMIT)
        logging.info("Extract data end. Total length {}.".format(len(data)))

        logging.info(f"Start transform data.")
        transform = Transform(data)
        data = transform.get_data()
        logging.info("Transform data successful end.")
        logging.info(f"Start load data to Elasticsearch")
        info = es_load.load_data(data)
        if updated_at is not None:
            state.set_state('updated_at', str(updated_at))
        logging.info(info)
        logging.info(f"Load data successful.")

        sleep(config.BULK_TIMER)


if __name__ == '__main__':
    main()
