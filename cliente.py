import pika
import utils
import sys

class cliente:
    def __init__(self):
        self.user_id = f"user_{sys.argv[0]}"
        self.private_key, self.public_key = utils.generate_keys()
        self.channel = utils.get_rabbitmq_channel()
        self.leiloes_disponiveis = {}
        self.leiloes_interessado =set()
        print(f"Cliente inicializado com id: {self.user_id}")