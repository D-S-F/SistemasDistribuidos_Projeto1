import utils
import uuid
import json
import threading

# Formato leilao
# ID: 
# {"descricao": "string",
# "inicio": datetime,
#"fim": datetime,
#"status": "agendado" || "ativo" || "finalizado"}

# Formato lance
# {"ID do leilao": id, 
# "ID do usuario": id,
# "valor": valor do lance,
# "assinatura": ass}

class ClienteLeilao:
    def __init__(self):
        self.user_id = f"user_{uuid.uuid4().hex[:6]}"
        self.private_key, self.public_key = utils.generate_keys()
        self.channel = utils.get_rabbitmq_channel()
        self.leiloes_disponiveis = {}
        self.leiloes_interessados = set()
        self.channel = utils.get_rabbitmq_channel()
        print(f"Cliente inicializado com id: {self.user_id}")

    def lance(self, id_leilao, valor):
        print("temp1")
    
    def checar_novos_leiloes(self):
        print("temp2")
        self.channel.queue_declare (queue= 'leilao_iniciado', durable=True)
        
        def callback_lance(ch , method, properties, body):
            data = json.loads(body)
            self.leiloes_disponiveis[data['id_leilao']] = data
            print("- Novo Leilão iniciado! -")
            print(f"ID: {data['id_leilao']} | {data['descricao']}\n")
            #ch.basic_ack(deliery_tag=method.delivery_tag)
        
        self.channel.basic_consume(queue='leilao_iniciado', on_message_callback=callback_lance)
        th_lance = threading.Thread(target=self.channel.start_consuming())
        th_lance.start()

    def checar_notif_leilao(self):
        print("temp3")

    def run(self):

        while True:
            print("\nEscolha: -(1)- Listar leilões disponíveis")
            print("-(1)- Listar leilões disponíveis")
            print("-(2)- Realizar um lance")
            print("-(3)- Encerrar cliente")
            modo = input("> ")

            if modo == '1':
                if not self.leiloes_disponiveis:
                    print("Não existem leilões disponiveis no momento")
                else:
                    for id_leilao, data in self.leiloes_disponiveis.items():
                        print(f"ID : {id_leilao} | {data['descricao']}")

            
            elif modo == '2':
                print("\n-Realização de lance-")
                id_leilao = input("Id do leilão em que deseja realziar o lance\n> ")
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
    
