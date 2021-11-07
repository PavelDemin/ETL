import json
from time import sleep
from load import Load
import misc
from transform import Transform
from extract import Extract
from config import config
from state import State, JsonFileStorage
import logging


es_load = Load()
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
    dsl = {
        "dbname": config.PG_DBNAME,
        "user": config.PG_USER,
        "password": config.PG_PASSWORD.get_secret_value(),
        "host": config.PG_HOST,
        "port": config.PG_PORT
    }
    table_list = ["film_work"]
    while True:
        for table in table_list:
            logging.info(f"Start extract data from Postgres server with limit {config.LIMIT}.")
            st = state.get_state(table)
            extract = Extract(dsl, config.LIMIT)
            data = extract.fetch_data(**st)
            logging.info("Extract data end. Total length {}.".format(len(data)))

            logging.info(f"Start transform data.")
            transform = Transform(data)
            data = transform.get_data()
            logging.info("Transform data successful end.")
            logging.info(f"Start load data to Elasticsearch")
            info = es_load.load_data(data)
            logging.info(info)
            logging.info(f"Load data successful.")

        sleep(5)


if __name__ == '__main__':
    main()
