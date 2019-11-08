from __future__ import print_function

import sys

from proton.handlers import MessagingHandler
from proton.reactor import Container


class Receiver(MessagingHandler):
    def __init__(self, conn_url, address):
        super(Receiver, self).__init__()
        self.conn_url = conn_url
        self.address = address

    def on_start(self, event):
        conn = event.container.connect(self.conn_url)
        event.container.create_receiver(conn, self.address)

    def on_link_opened(self, event):
        print("RECEIVE: Created receiver for source address '{0}'"
              .format(self.address))

    def on_message(self, event):
        message = event.message
        print("RECEIVE: Received message '{0}'".format(message.body))
        event.receiver.close()
        event.connection.close()


def main():
    conn_url = "amqp://localhost:5672"
    address = "sample.queue"

    receiver = Receiver(conn_url, address)
    container = Container(receiver)
    container.run()


main()
