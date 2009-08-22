from amqplib_thrift.transports import TAMQServerTransport, TAMQTransport
from thrift.protocol.TBinaryProtocol import TBinaryProtocol, \
    TBinaryProtocolFactory
from thrift.server.TServer import TSimpleServer
from thrift.transport.TTransport import TTransportFactoryBase, TMemoryBuffer

class TAMQInputTransportFactory(TTransportFactoryBase):
    """
    Input transport factory to be used with a TServer instance.
    """
    
    def getTransport(self, msg):
        """
        Creates and returns a TMemoryBuffer instance with the content of
        the amqp.Message instance passed as argument.
        """
        return TMemoryBuffer(msg.body)

class TAMQOutputTransportFactory(TTransportFactoryBase):
    """
    Output transport factory to be used with a TServer instance.
    """
    
    def __init__(self, channel, exchange):
        """
        Creates a new output factory instance for the given channel and
        exchange.
        """
        self.channel = channel
        self.exchange = exchange
        
    def getTransport(self, msg):
        """
        Creates and return a new TAMQTransport instance using the `reply_to`
        property of the amqp.Message argument to define the routing_key for
        the response.
        """
        routing_key = msg.properties['reply_to']
        return TAMQTransport(self.channel, self.exchange, routing_key)

class TAMQFactory(object):
    """
    Factory for AMQP based thrift clients and servers.
    """
    
    def __init__(self, channel, services_exchange='services',
        responses_exchange='responses', protocol_factory=None):
        """
        Creates a new factory operating over the given channel, sending
        messages to the given services and responses exchanges and using
        the protocols created by the given protocol factory.
        """
        self.channel = channel
        self.services_exchange = services_exchange
        self.responses_exchange = responses_exchange
        
        self.protocol_factory = protocol_factory or TBinaryProtocolFactory()
        
        self.channel.exchange_declare(self.services_exchange, "direct")
        self.channel.exchange_declare(self.responses_exchange, "direct")
    
    def get_client(self, client_class, routing_key):
        """
        Creates a new client for the given class sending messages to the
        hanlder with the given routing_key.
        """
        queue, _, _ = self.channel.queue_declare(exclusive=True,
            auto_delete=True)
        self.channel.queue_bind(queue, self.responses_exchange, queue)
        
        transport = TAMQTransport(self.channel, self.services_exchange,
            routing_key, queue)
        
        return client_class(self.protocol_factory.getProtocol(transport))
        
    def get_server(self, processor, routing_key, server_class=None):
        """
        Creates a new server for the given processor, using the `server_class`
        class (defaults to `TSimpleServer`) and listening to messages with the
        given routing key sent to the `services_exchange`
        """
        queue, _, _ = self.channel.queue_declare(exclusive=True,
            auto_delete=True)
        self.channel.queue_bind(queue, self.services_exchange, routing_key)
        
        server_class = server_class or TSimpleServer
        
        server_transport = TAMQServerTransport(self.channel, queue)
        
        itrans_factory = TAMQInputTransportFactory()
        otrans_factory = TAMQOutputTransportFactory(self.channel,
            self.responses_exchange)
        
        return server_class(processor, server_transport, itrans_factory,
            otrans_factory, self.protocol_factory, self.protocol_factory)