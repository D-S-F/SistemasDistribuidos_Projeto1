import pika
import json
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.exceptions import InvalidSignature

HOST = 'localhost'

def get_rabbitmq_channel():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=HOST))
    return connection.channel()

def setup_queues(channel):
    channel.queue_declare(queue='leilao_iniciado', durable=True)
    channel.queue_declare(queue='lance_realizado', durable=True)
    channel.queue_declare(queue='leilao_finalizado', durable=True)
    channel.queue_declare(queue='lance_validado', durable=True)
    channel.queue_declare(queue='leilao_vencedor', durable=True)
    
def generate_keys():
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public_key = private_key.public_key()
    return private_key, public_key

def sign_message(private_key, message):
    if isinstance(message, dict):
        message = json.dumps(message, sort_keys=True).encode('utf-8')
        
    signature = private_key.sign(
        message,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.MAX_LENGTH
        ),
        hashes.SHA256()
    )
    return signature

def verify_signature(public_key, signature, message):
    if isinstance(message, dict):
        message = json.dumps(message, sort_keys=True).encode('utf-8')

    try:
        public_key.verify(
            signature,
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH
            ),
            hashes.SHA256()
        )
        return True
    except InvalidSignature:
        return False

def serialize_public_key(public_key):
    """Converte uma chave pública para o formato PEM para transporte."""
    return public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    )

def deserialize_public_key(pem_data):
    """Converte uma chave pública do formato PEM de volta para um objeto."""
    return serialization.load_pem_public_key(pem_data)