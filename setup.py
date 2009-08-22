from distutils.core import setup

setup(
    name='amqplib_trift',
    version='0.1',
    description='Python library to use thrift services (client and servers) over AMQP with pyamqplib',
    author='Jonathan Stoppani',
    author_email='<jonathan.stoppani@edu.hefr.ch',
    url='http://github.com/garetjax/pyamqplib-thrift',
    download_url='http://github.com/garetjax/pyamqplib-thrift/tarball/master'
    packages=['amqplib_thrift'],
    license='MIT',
    requires=('amqplib',),
    provides=('amqplib_trift',),
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: MIT License',
        'Topic :: Internet',
        'Topic :: System :: Networking',
    ]
)