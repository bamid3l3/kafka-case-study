FROM python:3.9-slim

WORKDIR /app

COPY producer.py topics.py transformer.py messages.json tests ./

RUN pip install --no-cache-dir kafka-python pytz pytest

ENTRYPOINT ["tail", "-f", "/dev/null"]