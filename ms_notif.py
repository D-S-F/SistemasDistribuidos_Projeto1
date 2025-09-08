import pika
import utils
import json
import threading
import time
from typing import Dict, Any

class MSNotificacao:
    def __init__(self):
        self.channel = utils.get_rabbitmq_channel()
        self.setup_queues()
        self.running = True
        
        print("MS Notificação inicializado com sucesso!")
        print("📡 Escutando eventos das filas: lance_validado e leilao_vencedor")
    
    def setup_queues(self):
        """Configura as filas necessárias para o MS Notificação"""
        # Filas de entrada (subscriber)
        self.channel.queue_declare(queue='lance_validado', durable=True)
        self.channel.queue_declare(queue='leilao_vencedor', durable=True)
        
        # As filas específicas por leilão serão criadas dinamicamente
        print("✅ Filas de entrada configuradas!")
    
    def criar_fila_leilao(self, id_leilao: str):
        """Cria uma fila específica para um leilão se ela não existir"""
        nome_fila = f"leilao_{id_leilao}"
        self.channel.queue_declare(queue=nome_fila, durable=True)
        return nome_fila
    
    def processar_lance_validado(self, ch, method, properties, body):
        """Processa eventos de lance validado"""
        try:
            evento = json.loads(body)
            id_leilao = evento.get('id_leilao')
            
            if not id_leilao:
                print("❌ Erro: ID do leilão não encontrado no evento lance_validado")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            # Cria fila específica para o leilão
            fila_leilao = self.criar_fila_leilao(id_leilao)
            
            # Adiciona timestamp ao evento
            evento['timestamp'] = time.time()
            evento['tipo'] = 'lance_validado'
            
            # Publica na fila específica do leilão
            self.channel.basic_publish(
                exchange='',
                routing_key=fila_leilao,
                body=json.dumps(evento),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            
            print(f"📢 Lance validado roteado para leilão {id_leilao}")
            print(f"   💰 Usuário: {evento.get('id_usuario', 'N/A')}")
            print(f"   💵 Valor: R$ {evento.get('valor', 'N/A')}")
            
        except json.JSONDecodeError:
            print("❌ Erro: Falha ao decodificar JSON do evento lance_validado")
        except Exception as e:
            print(f"❌ Erro ao processar lance_validado: {e}")
        finally:
            ch.basic_ack(delivery_tag=method.delivery_tag)
    
    def processar_leilao_vencedor(self, ch, method, properties, body):
        """Processa eventos de leilão vencedor"""
        try:
            evento = json.loads(body)
            id_leilao = evento.get('id_leilao')
            
            if not id_leilao:
                print("❌ Erro: ID do leilão não encontrado no evento leilao_vencedor")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            # Cria fila específica para o leilão
            fila_leilao = self.criar_fila_leilao(id_leilao)
            
            # Adiciona timestamp ao evento
            evento['timestamp'] = time.time()
            evento['tipo'] = 'leilao_vencedor'
            
            # Publica na fila específica do leilão
            self.channel.basic_publish(
                exchange='',
                routing_key=fila_leilao,
                body=json.dumps(evento),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            
            print(f"🏆 Leilão vencedor roteado para leilão {id_leilao}")
            print(f"   👑 Vencedor: {evento.get('id_usuario', 'N/A')}")
            print(f"   💰 Valor final: R$ {evento.get('valor', 'N/A')}")
            
        except json.JSONDecodeError:
            print("❌ Erro: Falha ao decodificar JSON do evento leilao_vencedor")
        except Exception as e:
            print(f"❌ Erro ao processar leilao_vencedor: {e}")
        finally:
            ch.basic_ack(delivery_tag=method.delivery_tag)
    
    def consumir_lance_validado(self):
        """Thread para consumir eventos de lance validado"""
        try:
            self.channel.basic_consume(
                queue='lance_validado',
                on_message_callback=self.processar_lance_validado
            )
            print("🎯 Iniciando consumo da fila 'lance_validado'")
            self.channel.start_consuming()
        except Exception as e:
            if self.running:
                print(f"❌ Erro no consumo de lance_validado: {e}")
    
    def consumir_leilao_vencedor(self):
        """Thread para consumir eventos de leilão vencedor"""
        try:
            # Cria uma nova conexão para a segunda thread
            connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
            channel = connection.channel()
            channel.queue_declare(queue='leilao_vencedor', durable=True)
            
            channel.basic_consume(
                queue='leilao_vencedor',
                on_message_callback=self.processar_leilao_vencedor
            )
            print("🏆 Iniciando consumo da fila 'leilao_vencedor'")
            channel.start_consuming()
        except Exception as e:
            if self.running:
                print(f"❌ Erro no consumo de leilao_vencedor: {e}")
    
    def run(self):
        """Loop principal do MS Notificação"""
        print("\n🚀 MS Notificação iniciado!")
        print("📡 Escutando eventos e roteando para filas específicas...")
        print("Pressione Ctrl+C para parar o serviço")
        
        try:
            # Cria threads para consumir as duas filas simultaneamente
            thread_lance = threading.Thread(target=self.consumir_lance_validado, daemon=True)
            thread_vencedor = threading.Thread(target=self.consumir_leilao_vencedor, daemon=True)
            
            thread_lance.start()
            thread_vencedor.start()
            
            # Mantém o serviço rodando
            while self.running:
                time.sleep(1)
                
        except KeyboardInterrupt:
            print("\n🛑 MS Notificação encerrado pelo usuário")
            self.running = False
        except Exception as e:
            print(f"❌ Erro no MS Notificação: {e}")
            self.running = False

if __name__ == '__main__':
    ms_notif = MSNotificacao()
    ms_notif.run()
