import pika
import redis

r = redis.Redis()
r.set(0, 0)
r.set(1, 1)

def callback(ch, method, properties, body):
    n = int(body)
    print('received {}'.format(n))
    response = fibo(n)
    print('sent {}'.format(response))
    ch.basic_publish(exchange='', routing_key=properties.reply_to,
                     properties=pika.BasicProperties(correlation_id=properties.correlation_id),
                     body=str(response))
    ch.basic_ack(delivery_tag=method.delivery_tag)

def fibo(n):
    if r.get(n) is None:
        for i in range(2, n + 1):
            r.set(i, int(r.get(i-1)) + int(r.get(i-2)))
    return int(r.get(n))


connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
queue = 'rpc_queue'

channel.queue_declare(queue=queue)
channel.basic_qos(prefetch_count=1) # one message per receiver at a time
channel.basic_consume(callback, queue=queue)
channel.start_consuming()
