from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer
from kafka.admin import NewTopic
import pandas as pd
import json
from typing import List

class KafkaAPI:
    def __init__(self, bootstrap_servers: List[str]):
        self.bootstrap_servers = bootstrap_servers
        self.admin_client = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
        self.producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers,
                                      value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    def create_topic(self, topic_name: str):
        topic_list = [NewTopic(name=topic_name, num_partitions=1, replication_factor=1)]
        self.admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print(f"Topic '{topic_name}' created.")

    def delete_topic(self, topic_name: str):
        self.admin_client.delete_topics(topics=[topic_name])
        print(f"Topic '{topic_name}' deleted.")

    def write_data(self, topic_name: str, data: pd.DataFrame):
        for _, row in data.iterrows():
            self.producer.send(topic_name, row.to_dict())
        self.producer.flush()
        print(f"Data written to topic '{topic_name}'.")

    def read_data(self, topic_name: str, timeout_ms: int = 10000) -> pd.DataFrame:
        consumer = KafkaConsumer(topic_name,
                                 bootstrap_servers=self.bootstrap_servers,
                                 auto_offset_reset='earliest',
                                 enable_auto_commit=False,
                                 value_deserializer=lambda x: json.loads(x.decode('utf-8')))
        data = []
        consumer.poll(timeout_ms=timeout_ms)
        for message in consumer:
            data.append(message.value)
        consumer.close()
        return pd.DataFrame(data)

# Пример использования:
if __name__ == "__main__":
    bootstrap_servers = ['localhost:9092']  # Замените на ваш адрес Kafka сервера
    api = KafkaAPI(bootstrap_servers=bootstrap_servers)
    
    # Создание топика
    api.create_topic('test_topic')
    
    # Запись данных в топик
    df = pd.DataFrame({'key1': [1, 2, 3], 'key2': ['a', 'b', 'c']})
    api.write_data('test_topic', df)
    
    # Чтение данных из топика
    df_read = api.read_data('test_topic')
    print(df_read)
    
    # Удаление топика
    api.delete_topic('test_topic')
