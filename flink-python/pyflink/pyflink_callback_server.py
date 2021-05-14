import time

from pyflink.java_gateway import get_gateway


if __name__ == '__main__':
    # just a daemon process used to serve the rpc call from Java.
    gateway = get_gateway()
    watchdog = gateway.jvm.org.apache.flink.client.python.PythonGatewayServer.watchdog
    try:
        while watchdog.ping():
            time.sleep(1)
    finally:
        get_gateway().close()
        exit(0)
