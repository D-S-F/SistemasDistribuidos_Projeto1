import utils
import uuid
import json
import threading
import os

PUBLIC_KEYS_DIR = 'public_keys'

class ClienteLeilao:
    def __init__(self):
        self.user_id = f"user_{uuid.uuid4().hex[:6]}"
        self.private_key, self.public_key = utils.generate_keys()

        public_key_filename = os.path.join(PUBLIC_KEYS_DIR, f"{self.user_id}.pem")
        utils.save_key_to_file(self.public_key, public_key_filename)
        self.leiloes_disponiveis = {}
        self.leiloes_interessado = set()
        print(f"Cliente inicializado com id: {self.user_id}")

    def lance(self, id_leilao, valor):
        
        channel = utils.get_rabbitmq_channel()
        if id_leilao not in self.leiloes_disponiveis:
            print("\nID de leilão inválido!")
            return

        lance_info = {
            "id_leilao": id_leilao,
            "id_usuario": self.user_id,
            "valor": float(valor)
        }

        ass = utils.sign_message(self.private_key, lance_info)
        
        publication = {
            "data": lance_info,
            "assinatura": ass.hex()
        }

        channel.basic_publish(
            exchange='',
            routing_key='lance_realizado',
            body=json.dumps(publication)
        )
        print(f"Lance de R${valor} enviado para o leilão {id_leilao}.")
        

        if id_leilao not in self.leiloes_interessado:
            self.checar_notif_leilao(id_leilao)
            self.leiloes_interessado.add(id_leilao)
    
    def checar_novos_leiloes(self):
        
        channel = utils.get_rabbitmq_channel()
        channel.queue_declare (queue= 'leilao_iniciado', durable=True)
        
        def callback_novo_leilao(ch , method, properties, body):
            if not body:
                return
            try:
                data = json.loads(body)
                leilao_id = data.get('id_leilao')
                if leilao_id:
                    self.leiloes_disponiveis[leilao_id] = data
                    print(f"\nNovo leião disponível! ID: {leilao_id} | {data.get('descricao')}")
            except json.JSONDecodeError:
                print(f"Erro ao decodificar JSON") 
        
        result = channel.queue_declare(queue='', exclusive=True)
        queue_name_iniciado = result.method.queue
        channel.queue_bind(exchange='leilao_iniciado', queue=queue_name_iniciado)
        channel.basic_consume(queue=queue_name_iniciado, on_message_callback=callback_novo_leilao, auto_ack=True)
        th_lance = threading.Thread(target=channel.start_consuming, daemon=True)
        th_lance.start()

    def checar_notif_leilao(self, id_leilao):

        channel = utils.get_rabbitmq_channel()
        result = channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue
        
        queue_key = f"leilao.{id_leilao}"
        channel.queue_bind(exchange='notificacao_leilao', queue=queue_name, routing_key=queue_key)
        
        def callback_notificacao(ch, method, properties, body):
            if not body:
                return
            data = json.loads(body)
            notif_id_leilao = data.get('id_leilao')
            print(f"\n--- NOTIFICAÇÃO DO LEILÃO {notif_id_leilao} ---")
            if "id_vencedor" in data:
                print(f"||LEILÃO ENCERRADO!||\nVencedor: {data['id_vencedor']} com R${data['valor']}.")
                self.leiloes_disponiveis.pop(notif_id_leilao)
            else:
                print(f"Novo lance foi realizado!\nUsuário: {data['id_usuario']}, Valor: R${data['valor']}.")
            print("---------------------------------")
        def consume_thread():
            channel.basic_consume(queue=queue_name, on_message_callback=callback_notificacao, auto_ack=True)
            channel.start_consuming()
        
        th_notif = threading.Thread(target=consume_thread, daemon=True)
        th_notif.start()
        print(f"Inscrito para receber notificações do leilão {id_leilao}.")

    def run(self):
        self.checar_novos_leiloes()
        while True:
            print("\nEscolha:")
            print("-(1)- Listar leilões disponíveis")
            print("-(2)- Realizar um lance")
            print("-(3)- Encerrar cliente")
            modo = input("> ")

            if modo == '1':
                if not self.leiloes_disponiveis:
                    print("Não existem leilões disponíveis no momento")
                else:
                    for id_leilao, data in self.leiloes_disponiveis.items():
                        print(f"ID : {id_leilao} | {data['descricao']}")

            
            elif modo == '2':
                print("\n-Realização de lance-")
                id_leilao = input("Id do leilão em que deseja realizar o lance\n> ")
                valor = input(f"Qual o valor do lance no leilão {id_leilao}?\n> ")
                self.lance(id_leilao, valor)

            elif modo == '3':
                break

            else:
                print("\n||Opção invalida||")

        print("\nCliente encerrado")



if __name__ == '__main__':
    cliente = ClienteLeilao()
    cliente.run()
    
