import sys
import os
from amqplib_thrift.factories import TAMQFactory
from amqplib import client_0_8 as amqp

# Set up sys.path to include thrift services
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'gen-py'))

# Thrift services
from tutorial import Calculator
from tutorial.ttypes import Work, Operation, InvalidOperation

# Set up connection
connection = amqp.Connection(host="10.0.0.100")
channel = connection.channel()

# Set up factory
factory = TAMQFactory(channel)

# Get the client
calculator = factory.get_client(Calculator.Client, 'calculator')

# Calculate something
calculator.ping()

print "4 + 5 = ", calculator.add(4, 5)

w = Work(num1=20, num2=10, op=Operation.MULTIPLY)
print "20 * 10 = ", calculator.calculate(1, w)

w = Work(num1=2, num2=0, op=Operation.DIVIDE)
try:
    result = calculator.calculate(2, w)
except InvalidOperation, e:
    print "Operation %d failed with message '%s'" % (e.what, e.why)
else:
    print "2 / 0 = ", result

print "Zipping...",
calculator.zip()
print "done"

print "Pinging...",
calculator.ping()
print "done"

# Tear down the connection
channel.close()
connection.close()