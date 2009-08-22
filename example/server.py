import sys
import os
from amqplib_thrift.factories import TAMQFactory
from amqplib import client_0_8 as amqp
from thrift.server.TServer import TForkingServer

# Set up sys.path to include thrift services
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'gen-py'))

# Thrift services
from tutorial import Calculator
from tutorial.ttypes import Operation, Work, InvalidOperation

# Set up connection
connection = amqp.Connection(host="10.0.0.100")
channel = connection.channel()

class CalculatorHandler(Calculator.Iface):
    operations = {
        Operation.ADD: int.__add__,
        Operation.SUBTRACT: int.__sub__,
        Operation.MULTIPLY: int.__mul__,
        Operation.DIVIDE: int.__div__,
    }
    
    def ping(self):
        print "ping() called from client"
    
    def add(self, num1, num2):
        print "add(num1, num2) called from client"
        return num1 + num2
    
    def calculate(self, logid, w):
        print "calculate(logid, w) called from client"
        try:
            return self.operations[w.op](w.num1, w.num2)
        except ZeroDivisionError, e:
            raise InvalidOperation(what=logid, why=e.args[0]) # e.message is deprecated!
    
    def zip(self):
        print "zip() called from client"
        
        import time
        time.sleep(10) # Long running process
        print "zip() finished"

processor = Calculator.Processor(CalculatorHandler())

factory = TAMQFactory(channel)
# Using the get_server method without a server_class argument is not
# reccomended as it blocks all processing for long lasting handler methods.
# The TForkingServer used here, does not work on windows.
# The thrift.server.TServer module offers other implementation of the TServe
# class which can be used here.
server = factory.get_server(processor, 'calculator', TForkingServer)

try:
    print "Starting server"
    server.serve()
except KeyboardInterrupt:
    print "Shutting down server"
    
    # Tear down the connection
    channel.close()
    connection.close()