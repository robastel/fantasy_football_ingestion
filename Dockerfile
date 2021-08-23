FROM python:3.8-slim

RUN python -m pip install --upgrade pip

RUN mkdir -p /app

COPY ./requirements.txt /app/
RUN pip install -r /app/requirements.txt

COPY ./entrypoint.sh /app/
COPY ./main.py /app/
COPY ./src/ /app/src/
COPY ./config.yaml /app/

WORKDIR /app/

ENTRYPOINT ["/bin/bash", "entrypoint.sh"]