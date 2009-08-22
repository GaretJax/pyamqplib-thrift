from cStringIO import StringIO
from amqplib import client_0_8 as amqp
from thrift.transport.TTransport import TTransportBase, TServerTransportBase

class TAMQServerTransport(TServerTransportBase):
    """
    An implementation of TServerTransportBase to support AMQP messaging.
    """
    
    def __init__(self, channel, queue):
        """
        Creates a new AMQP ServerTransport for the given channel and listening
        on the given queue.
        """
        self.channel = channel
        self.queue = queue
        self.msg = None
    
    def listen(self):
        """
        Starts listening.
        """
        self.channel.basic_consume(self.queue, callback=self.incomingMessage,
            no_ack=True)
    
    def accept(self):
        """
        Waits for a message to be dispatched to the queue and returns it.
        If a message was already received, returns immediately.
        """
        if not self.msg:
            self.channel.wait()
        
        msg = self.msg
        self.msg = None
        
        return msg
    
    def incomingMessage(self, msg):
        """
        Callback for the `basic_consume` method. Sets the `msg` istance
        property to the received amqp.Message instance.
        """
        self.msg = msg

class TAMQTransport(TTransportBase):
    """
    AMQP transport implementation.
    """
    
    def __init__(self, channel, exchange, routing_key, queue=None):
        """
        Creates a new TAMQTransport instance.
        
        @param channel: The AMQP channel to use for the client-server
                        communication
        @type  channel: an L{amqp.Channel}
        
        @param exchange: The name of the exchange to which send messages.
        @type  exchange: a C{string}
        
        @param routing_key: The rouring_key to use for outgoing messages.
        @type  routing_key: a C{string}
        
        @param queue: The queue on which to listen when used for clients.
                      This is the response queue for non `oneway` requests.
                      If setted, the `reply_to` property of outgoing messages
                      will be setted to the queue name.
        @type  queue: a C{string}
        """
        
        # Input and output buffers
        self.__wbuf = StringIO()
        self.__rbuf = None
        
        self.channel = channel
        self.exchange = exchange
        self.routing_key = routing_key
        self.reply_to = queue
        
        if queue:
            channel.basic_consume(queue, callback=self._incoming_message,
                no_ack=True)
    
    def read(self, sz):
        """
        Read sz bytes from the input buffer.
        If the input buffer is empty, wait for data.
        """
        if not self.__rbuf:
            self.channel.wait()
        
        chunk = self.__rbuf.read(sz)
        
        if not len(chunk):
            # Buffer is empty, time to listen for the next message
            self.__rbuf = None
            return self.read(sz)
        
        return chunk
    
    def write(self, buf):
        """
        Writes some data to the output buffer.
        """
        self.__wbuf.write(buf)
    
    def flush(self):
        """
        Sends the message and clears the output buffer.
        """
        message = self.__wbuf.getvalue()
        self.__wbuf = StringIO()
        
        kwargs = {'application_headers': {
            'thriftClientName' : self.routing_key
        }}
        
        if self.reply_to:
            kwargs['reply_to'] = self.reply_to
        
        msg = amqp.Message(message, **kwargs)
        
        self.channel.basic_publish(msg, self.exchange, self.routing_key)
    
    def _incoming_message(self, msg):
        """
        Callback for incoming messages. 
        """
        self.__rbuf = StringIO(msg.body)
    
    