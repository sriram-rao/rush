from snakebite.client import Client


class HDFSClient:
    __client = None
    def __init__(self):
        self.client = Client("localhost", 9000)
    @staticmethod
    def get_instance(self):
    def test(self):
        for x in self.client.ls(['/rush/input/']):
            print (x)

