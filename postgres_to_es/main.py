import json
import logging
from time import sleep

from config import config
from extract import Extract
from load import Load
from state import JsonFileStorage, State
from transform import Transform


logger = logging.getLogger('ETL')
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch = logging.StreamHandler()
ch.setLevel(config.LOGGER_LEVEL)
ch.setFormatter(formatter)
logger.addHandler(ch)


pg_dsl = {
    "database": config.PG_DBNAME,
    "user": config.PG_USER,
    "password": config.PG_PASSWORD.get_secret_value(),
    "host": config.PG_HOST,
    "port": config.PG_PORT,
    "options": "-c search_path={}".format(config.PG_SCHEMA)
}

el_dsl = {'host': config.ES_HOST, "port": config.ES_PORT}


def create_indices(es_load):
    """
    This method create index to elasticsearch database if index is not exist
    """
    with open(config.INDICES_FILE_PATH, "r") as file:
        indices: dict = json.load(file)
        for index in indices.keys():
            if es_load.cat_index(index) is False:
                logger.info(f"Index {index} not found!")
                es_load.crate_index(index, indices[index])
                logger.info("Index {index} create successful.")


def main():
    extract = Extract(pg_dsl, config.LIMIT)
    es_load = Load(el_dsl)
    state = State(JsonFileStorage(config.STATE_FILE_PATH))

    create_indices(es_load)

    while True:
        logger.info(f"Start extract data from Postgres server with limit {config.LIMIT}.")
        st = state.get_state('updated_at')
        data, updated_at = extract.fetch_data(st, config.LIMIT)
        logger.info("Extract data end. Total length {}.".format(len(data)))
        if len(data) == 0:
            sleep(config.BULK_TIMER)
            continue
        logger.info(f"Start transform data.")
        transform = Transform(data)
        data = transform.get_data()
        logger.info("Transform data successful end.")

        logger.info(f"Start load data to Elasticsearch")
        info = es_load.load_data(data)
        if updated_at is not None:
            state.set_state('updated_at', str(updated_at))
        logger.info(info)
        logger.info(f"Load data successful.")

        sleep(config.BULK_TIMER)


if __name__ == '__main__':
    main()
