FROM python:3.9.7-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /etl/postgres_to_es
COPY pyproject.toml poetry.lock /etl/
RUN pip3 install poetry
ENV POETRY_VIRTUALENVS_CREATE false
COPY . /etl
RUN poetry install

CMD ["python", "main.py"]