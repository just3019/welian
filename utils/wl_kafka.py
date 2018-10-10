from kafka import KafkaProducer
import json

server = ['192.168.1.240:9092', ]
topic = 'distribution.test'


# 文档地址  https://github.com/dpkp/kafka-python
class Producer:
    def __init__(self, server_list: list):
        self.server = server_list
        self.kafka_producer = KafkaProducer(bootstrap_servers=server)

    def send(self, topic, key, data, data_type=None):
        if data_type == 'json':
            data = json.dumps(data)
        data = data.encode('utf-8')
        self.kafka_producer.send(topic=topic, key=key, value=data)


if __name__ == '__main__':
    print(Producer(server).send(topic=topic, key="thahah".encode("utf-8"), data='hahahah'))
