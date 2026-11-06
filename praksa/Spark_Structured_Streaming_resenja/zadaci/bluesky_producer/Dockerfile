FROM python:3.12-slim

WORKDIR /usr/src/app

COPY . .
RUN pip install --no-cache-dir -r requirements.txt

ENV KAFKA_BROKER="kafka1:19092"
ENV TOPIC="subreddit-bluesky-en"

CMD ["python", "-u", "./producer.py"]