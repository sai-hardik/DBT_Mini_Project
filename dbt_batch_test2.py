#!/usr/bin/python3

# imports
from kafka import KafkaProducer  # pip install kafka-python
import numpy as np  # pip install numpy
from sys import argv, exit
from time import time, sleep

# different device "profiles" with different
# distributions of values to make things interesting
# tuple --> (mean, std.dev)
DEVICE_PROFILES = {
    "Bitcoin": {'size': (51.3, 17.7), 'fee': (77.4, 18.7), 'volume': (1019.9, 9.5)},
    "Ethereum": {'size': (49.5, 19.3), 'fee': (33.0, 13.9), 'volume': (1012.0, 41.3)},
    "Dogecoin": {'size': (63.9, 11.7), 'fee': (62.8, 21.8), 'volume': (1015.9, 11.3)},
}

# check for arguments, exit if wrong
if len(argv) != 2 or argv[1] not in DEVICE_PROFILES.keys():
    print("please provide a valid device name:")
    for key in DEVICE_PROFILES.keys():
        print(f"  * {key}")
    print(f"\nformat: {argv[0]} DEVICE_NAME")
    exit(1)

profile_name = argv[1]
profile = DEVICE_PROFILES[profile_name]

# set up the producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

count = 1

# Define sliding window duration and interval
window_duration = 10  # seconds
interval = 2  # seconds
batch_size = 5
n_samples = int(window_duration / interval)
n_batches = int(n_samples / batch_size)

# List to hold samples for the current batch
samples = []

# until ^C
while True:
    # get random values within a normal distribution of the value
    size = np.random.normal(profile['size'][0], profile['size'][1])
    fee = max(0, min(np.random.normal(profile['fee'][0], profile['fee'][1]), 100))
    volume = np.random.normal(profile['volume'][0], profile['volume'][1])

    # create CSV structure
    msg = f'{time()},{profile_name},{size},{fee},{volume}'

    # Add sample to the list
    samples.append(msg)

    # Send samples to Kafka if the batch is complete
    if len(samples) == batch_size:
        batch_msgs = ','.join(samples)
        producer.send('cryptoB', bytes(batch_msgs, encoding='utf8'))
        count += 1
        print(f'sending data to kafka, #{count}')

        # Empty the list for the next batch
        samples = []

    # Sleep for the interval duration
    sleep(interval)

    # Check if the window is complete and send remaining samples as a batch
    if len(samples) > 0 and len(samples) % batch_size == 0:
        batch_msgs = ','.join(samples)
        producer.send('cryptoB', bytes(batch_msgs, encoding='utf8'))
        count += 1
        print(f'sending data to kafka, #{count}')

        # Empty the list for the next batch
        samples = []

    # Check if the window is complete and send remaining samples as a batch
    if count % n_batches == 0:
        # Sleep for the remaining window duration
        sleep(window_duration - (n_batches * interval * batch_size))
