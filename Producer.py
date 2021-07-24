import asyncio
import sys

from aiokafka import AIOKafkaProducer


async def stream_data(topic, bootstrap_servers, resource_path):
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
    # Get cluster layout and initial topic/partition leadership information
    await producer.start()
    try:
        f = open(resource_path)
        line = f.readline()
        while line:
            await producer.send_and_wait(topic, bytes(line, 'utf-8'))
            await asyncio.sleep(.4)  # Every 400ms. 2 messages in 1s
            line = f.readline()  # Next line
    finally:
        # Wait for all pending messages to be delivered or expire.
        await producer.stop()


async def main():
    print(sys.argv)
    await stream_data("access-logs", "localhost:9092", sys.argv[1])


asyncio.run(main())
