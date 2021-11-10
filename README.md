# Проектное задание: ETL

## Установка

1. Склонировать проект командой `git clone https://github.com/PavelDemin/ETL.git`
2. В корне проекта создать конфигурационный файл `.env` со следующим содержимым:
   1. `SECRET_KEY="указать секретный ключ"`
   2. `DEBUG=1` - 1 - включен debug, 0 - выключен debug
   3. `PG_DBNAME="movies"` - имя базы данных
   4. `PG_PASSWORD="qwerty123"` - пароль к базе данных
   5. `PG_HOST="postgres-serv"` - хост базы данных
   6. `PG_PORT=5432` - стандартный порт
   7. `PG_USER="postgres"` - имя пользователя базы данных
   8. `PG_SCHEMA="content"` - схема базы данных
   9. `LIMIT=1000` - размер одной пачки данных с постргреса
   10. `BULK_TIMER=5` - время цикла опроса постгреса
   11. `STATE_FILE_PATH="state.json"` - путь до state.json с ключем "updated_at": "2000-01-01 00:00:00.000000+00:00"
   12. `INDICES_FILE_PATH="indices.json"` - путь до indices.json со схемой индекса
   13. `ES_HOST="elastic-serv"` - хост elasticsearch
   14. `ES_PORT=9200` - порт elasticsearch
   15. `LOGGER_LEVEL="INFO"` - уровень логгера
3. Запустить контейнеры командой `docker-compose up -d`
4. ETL запущен!
