from kafka import KafkaProducer
import time

def create_producer():
    return KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: v.encode('utf-8')
    )

def main():
    producer = create_producer()
    topic = 'raw-flights'

    print(f"Starting producer - sending data to topic '{topic}'")

    with open('flights_summary.json', 'r') as file:
        for line_number, line in enumerate(file, 1):
            line = line.strip()
            if line:
                producer.send(topic, value=line)
                print(f"Sent line {line_number}: {line[:80]}...")
                time.sleep(0.1)

    producer.flush()
    producer.close()
    print("Producer finished sending all messages")

if __name__ == "__main__":
    main()
