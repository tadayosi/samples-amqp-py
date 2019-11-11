from __future__ import print_function

import sys

from proton import Message
from proton import symbol
from proton.handlers import MessagingHandler
from proton.reactor import Container


class Sender(MessagingHandler):
    def __init__(self, conn_url, address, message_body):
        super(Sender, self).__init__()
        self.conn_url = conn_url
        self.address = address
        self.message_body = message_body

    def on_start(self, event):
        conn = event.container.connect(self.conn_url)
        event.container.create_sender(conn, self.address)

    def on_link_opened(self, event):
        print("on_link_opened: address  = {0}".format(
            event.sender.target.address))

    def on_sendable(self, event):
        message = Message(self.message_body)
        message.annotations = {
            symbol("aaa"): "xxx",
            symbol("bbb"): "yyy",
            symbol("ccc"): "zzz"
        }
        event.sender.send(message)
        print("on_sendable: body        = {0}".format(message.body))
        print("on_sendable: annotations = {0}".format(message.annotations))
        event.sender.close()
        event.connection.close()


def main():
    conn_url = "amqp://localhost:5672"
    address = "sample.queue"
    message_body = "Hello!"

    sender = Sender(conn_url, address, message_body)
    container = Container(sender)
    container.run()


main()
