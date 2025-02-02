import os
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka import KafkaConsumer

import requests
import logging
import json
import aiohttp


# 웹 크롤링
async def get_real_infomation_async():
    load_dotenv()
    # year_data = 2025
    get_url = os.getenv("OPENAPI_URL")
    openkeys = os.getenv("OPENAPI_KEY")

    try:
        async with aiohttp.ClientSession() as session:
            openUrl = f"{get_url}{openkeys}/json/tbLnOpendataRtmsV/1/1000/"
            async with session.get(openUrl) as rep:
                if rep.status == 200:
                    content = await rep.json()
                    return content
    except aiohttp.ClientError as err:
        logging.error("reponse err", err)
    except json.JSONDecodeError as e:
        logging.error(f"error", {e})


# apache kafka message queue
async def send_data_message_async(data):
    producer = None
    try:
        kafka_config = {
            "bootstrap_servers": ["localhost:9092"],
            "value_serializer": lambda x: json.dumps(x).encode("utf-8"),
            "acks": 1,
            "retries": 3,
            "linger_ms": 10,
            "batch_size": 16384,
        }
        producer = KafkaProducer(**kafka_config)
        # print(producer.bootstrap_connected())
        producer.send("test-topic", data)
    except Exception as e:
        logging.error(f"Error date to kafka, {e}")
    finally:
        if producer is not None:
            producer.flush()
            producer.close()


async def process_data():
    information = await get_real_infomation_async()
    if information:
        await send_data_message_async(information)
        print(information)


# async def process_recv():

#     topic = "recv-topic"
#     configs = {
#         "bootstrap_servers": ["localhost:9092"],
#         "auto_offset_reset": "earliest",
#     }
#     consumer = KafkaConsumer(**configs)
#     consumer.subscribe(topics=topic)
#     for msg in consumer:
#         print(msg.value)
#     consumer.close()


# # answer = answer_quest(question=quest)
# information = get_real_infomation()
# if information:
#     send_data_message(information)
#     # recv_data_message()
