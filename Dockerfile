FROM python:3.9-alpine

COPY . /app
WORKDIR /app

RUN python -m pip install --upgrade pip

RUN pip install pipenv

RUN pipenv install --system --deploy

CMD ["python", "coviddata.py"]
