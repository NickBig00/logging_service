import datetime
import json
import os

import pika

EXCHANGE_NAME = "event_log"


def callback(ch, method, properties, body):
    data = json.loads(body)

    timestamp = datetime.datetime.now()
    log_entry = f"[{timestamp}] {json.dumps(data)}\n"
    print(f"Received log: {log_entry.strip()}")

    with open("central_log.txt", "a") as f:
        f.write(log_entry)


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters("rabbitmq"))
    channel = connection.channel()

    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="topic")

    result = channel.queue_declare(queue="central_logging_queue", durable=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange=EXCHANGE_NAME, queue=queue_name, routing_key="log.*")

    print("Logging Service is running... waiting for messages (log.*)")
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    channel.start_consuming()


if __name__ == "__main__":
    main()
