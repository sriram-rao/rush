import threading


def configure():
    context = threading.local()
    context.name = "Main"
