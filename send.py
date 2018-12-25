import sys
import pika
import uuid


class FibonacciRpcClient(object):
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()
        result = self.channel.queue_declare(exclusive=True)
        self.queue = result.method.queue

        self.channel.basic_consume(self.callback, no_ack=True, queue=self.queue)

    def callback(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, n):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(exchange='', routing_key=routing_key,
                                   properties=pika.BasicProperties(reply_to=self.queue, correlation_id=self.corr_id,),
                                   body=str(n))
        while not self.response:
            self.connection.process_data_events()

        return int(self.response)

message = sys.argv[1] if len(sys.argv) > 1 else "4"
routing_key = 'rpc_queue'

fibo_rpc = FibonacciRpcClient()
print('send {}'.format(message))
response = fibo_rpc.call(message)
print('receive {}'.format(response))
