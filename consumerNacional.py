from confluent_kafka import Consumer, KafkaError
import socket
import uuid

conf = {
    'bootstrap.servers': 'pkc-4r087.us-west2.gcp.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'DDUTHVQCYDVVQOXM',
    'sasl.password': 'sexksNS0i9bwEDEpW3mEGEAzTIov7FCQimL7EMR1riY56EaNL/0X9lntltrP5V+c',
    'group.id': str(uuid.uuid4()),
    'auto.offset.reset': 'earliest'
}


consumer = Consumer(conf)

consumer.subscribe(['Nacional'])

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break
        print('Received message: {}'.format(msg.value().decode('utf-8')))

except KeyboardInterrupt:
    pass

finally:
    consumer.close()
