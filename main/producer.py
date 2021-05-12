import pika, json

params = pika.URLParameters('amqps://flynhnqj:EpVX2LXRTBUsv8e6AYE6aIFQQESpCZ5R@puffin.rmq2.cloudamqp.com/flynhnqj')

connection = pika.BlockingConnection(params)

channel = connection.channel()


def publish(method, body):
    properties = pika.BasicProperties(method)
    channel.basic_publish(exchange='', routing_key='admin', body=json.dumps(body), properties=properties)
