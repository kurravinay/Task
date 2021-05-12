# RMQ consumer to add the data to the app based on the event
import pika, json

from main import Product, db

params = pika.URLParameters('amqps://flynhnqj:EpVX2LXRTBUsv8e6AYE6aIFQQESpCZ5R@puffin.rmq2.cloudamqp.com/flynhnqj')

connection = pika.BlockingConnection(params)

channel = connection.channel()

channel.queue_declare(queue='main')


def callback(ch, method, properties, body):
    print('Received in main')
    data = json.loads(body)
    print(data)
# adding the data to the app as the product added in the background
    if properties.content_type == 'product_created':
        product = Product(id=data['id'], title=data['title'], image=data['image'])
        db.session.add(product)
        db.session.commit()
        print('Product Created')
# updating the data to the app as the product updated in the background
    elif properties.content_type == 'product_updated':
        product = Product.query.get(data['id'])
        product.title = data['title']
        product.image = data['image']
        db.session.commit()
        print('Product Updated')
# deleting the data to the app as the product deleted in the background
    elif properties.content_type == 'product_deleted':
        product = Product.query.get(data)
        db.session.delete(product)
        db.session.commit()
        print('Product Deleted')


channel.basic_consume(queue='main', on_message_callback=callback, auto_ack=True)

print('Started Consuming')

channel.start_consuming()

channel.close()
