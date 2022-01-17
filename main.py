import datetime
import json
import random
from time import sleep

import boto3
from dotenv import load_dotenv

DATE_FORMAT = '%Y-%m-%dT%H:%M:%SZ'


def publish(sqs):
    # Send message to SQS queue
    sent_dt = datetime.datetime.now().strftime(DATE_FORMAT)
    body = {"timestamp": sent_dt,
            "payload": {"eventType": "DRIVER_LOCATION_UPDATED", "driverId": "f0ef7677-f7c6-4f1a-8836-14c08c954f14",
                        "coordinates": {"lat": 49.1987689, "lon": 16.5766822, "accuracy": 16.242000579833984}}}
    response = sqs.send_message(
        QueueUrl=queue_url,
        MessageAttributes={
            'Title': {
                'DataType': 'String',
                'StringValue': 'The Whistler'
            },
            'Author': {
                'DataType': 'String',
                'StringValue': 'John Grisham'
            },
            'WeeksOn': {
                'DataType': 'Number',
                'StringValue': '6'
            }
        },
        MessageBody=(
            f'{json.dumps(body)}'
        )
    )

    print(f"{sent_dt} publishing {response['MessageId']}")


def receive(sqs):
    # Receive message from SQS queue
    try:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MessageAttributeNames=['requestId'],
            MaxNumberOfMessages=10,
            WaitTimeSeconds=20
        )
    except BaseException as e:
        print(e)
        return

    message = response['Messages'][0]
    receipt_handle = message['ReceiptHandle']
    msg_body = json.loads(message['Body'])
    sent_timestamp = msg_body['timestamp']
    sent_dt = datetime.datetime.strptime(sent_timestamp, DATE_FORMAT)

    # Delete received message from queue
    sqs.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle
    )
    receive_dt = datetime.datetime.now()
    print(f'latency: {receive_dt - sent_dt}')
    print(f'{datetime.datetime.now()} Received and deleted message %s' % message)


if __name__ == '__main__':
    load_dotenv()

    # Create SQS client
    sqs = boto3.client('sqs')
    queue_url = 'dummy-flow-allocation-svc'
    publish(sqs)
    while True:
        receive(sqs)
        sleep(random.randint(1, 5))
        publish(sqs)
