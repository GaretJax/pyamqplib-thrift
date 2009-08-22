from distutils.core import setup

setup(
    name='pyamqplib-trift',
    version='0.1',
    description='Python library to use thrift services (client and servers) over AMQP with pyamqplib',
    author='Jonathan Stoppani',
    author='Jonathan Stoppani',
    author_email='<jonathan.stoppani@edu.hefr.ch',
    url='http://github.com/garetjax/pyamqplib-thrift',
    packages=['amqplib_thrift'],
    license='MIT',
    requires=('amqplib',),
    provides=('pyamqplib-trift',),
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: MIT License',
        'Topic :: Internet',
        'Topic :: System :: Networking',
    ]
)