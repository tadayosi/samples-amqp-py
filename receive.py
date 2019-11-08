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
        print("on_link_opened: address  = {0}".format(self.address))

    def on_message(self, event):
        message = event.message
        print("on_message: body         = {0}".format(message.body))
        print("on_message: annotations  = {0}".format(message.annotations))
        print("on_message: instructions = {0}".format(message.instructions))
        print("on_message: properties   = {0}".format(message.properties))
        event.receiver.close()
        event.connection.close()


def main():
    conn_url = "amqp://localhost:5672"
    address = "sample.queue"

    receiver = Receiver(conn_url, address)
    container = Container(receiver)
    container.run()


main()
