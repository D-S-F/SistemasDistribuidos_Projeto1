import pika
import utils
import json
import datetime
import time
import threading
from typing import Dict, List

class MSLeilao:
    def __init__(self):
        self.channel = utils.get_rabbitmq_channel()
        self.setup_queues()
        
        # Lista pré-configurada (hardcoded) de leilões
        self.leiloes = {
            "leilao_001": {
                "descricao": "iPhone 15 Pro Max 256GB - Azul Titânio",
                "inicio": datetime.datetime(2025, 9, 8, 18, 45, 0),  # 15/01/2024 14:00
                "fim": datetime.datetime(2025, 9, 8, 18, 47, 0),    # 15/01/2024 16:00
                "status": "agendado"
            },
            "leilao_002": {
                "descricao": "MacBook Air M2 13'' 512GB - Meia-noite",
                "inicio": datetime.datetime(2024, 1, 16, 10, 0, 0),  # 16/01/2024 10:00
                "fim": datetime.datetime(2024, 1, 16, 12, 0, 0),     # 16/01/2024 12:00
                "status": "agendado"
            },
            "leilao_003": {
                "descricao": "Samsung Galaxy S24 Ultra 512GB - Preto",
                "inicio": datetime.datetime(2024, 1, 17, 15, 30, 0), # 17/01/2024 15:30
                "fim": datetime.datetime(2024, 1, 17, 17, 30, 0),    # 17/01/2024 17:30
                "status": "agendado"
            },
            "leilao_004": {
                "descricao": "PlayStation 5 + 2 Controles + 3 Jogos",
                "inicio": datetime.datetime(2024, 1, 18, 20, 0, 0),  # 18/01/2024 20:00
                "fim": datetime.datetime(2024, 1, 18, 22, 0, 0),     # 18/01/2024 22:00
                "status": "agendado"
            },
            "leilao_005": {
                "descricao": "Nintendo Switch OLED + 5 Jogos Exclusivos",
                "inicio": datetime.datetime(2024, 1, 19, 19, 0, 0),  # 19/01/2024 19:00
                "fim": datetime.datetime(2024, 1, 19, 21, 0, 0),     # 19/01/2024 21:00
                "status": "agendado"
            },
            "leilao_006": {
                "descricao": "sla é um teste mr stec",
                "inicio": datetime.datetime(2025, 1, 19, 19, 0, 0),  # 19/01/2024 19:00
                "fim": datetime.datetime(2025, 11, 19, 21, 0, 0),     # 19/01/2024 21:00
                "status": "agendado"
            }
        }
        
        print("MS Leilão inicializado com sucesso!")
        print(f"Total de leilões cadastrados: {len(self.leiloes)}")
    
    def setup_queues(self):
        """Configura as filas necessárias para o MS Leilão"""
        utils.setup_queues(self.channel)
        print("Filas configuradas com sucesso!")
    
    def iniciar_leilao(self, id_leilao: str):
        """Inicia um leilão e publica o evento"""
        if id_leilao not in self.leiloes:
            print(f"Erro: Leilão {id_leilao} não encontrado!")
            return
        
        leilao = self.leiloes[id_leilao]
        leilao["status"] = "ativo"
        
        # Dados do evento de leilão iniciado
        evento = {
            "id_leilao": id_leilao,
            "descricao": leilao["descricao"],
            "inicio": leilao["inicio"].isoformat(),
            "fim": leilao["fim"].isoformat(),
            "status": "ativo"
        }
        
        # Publica o evento na fila
        self.channel.basic_publish(
            exchange='leilao_iniciado',
            routing_key='',
            body=json.dumps(evento),
        )
        
        print(f"✅ Leilão {id_leilao} iniciado: {leilao['descricao']}")
    
    def finalizar_leilao(self, id_leilao: str):
        """Finaliza um leilão e publica o evento"""
        if id_leilao not in self.leiloes:
            print(f"Erro: Leilão {id_leilao} não encontrado!")
            return
        
        leilao = self.leiloes[id_leilao]
        leilao["status"] = "finalizado"
        
        # Dados do evento de leilão finalizado
        evento = {
            "id_leilao": id_leilao,
            "descricao": leilao["descricao"],
            "status": "finalizado",
            "fim": leilao["fim"].isoformat()
        }
        
        # Publica o evento na fila
        self.channel.basic_publish(
            exchange='',
            routing_key='leilao_finalizado',
            body=json.dumps(evento),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        
        print(f"🏁 Leilão {id_leilao} finalizado: {leilao['descricao']}")
    
    def verificar_leiloes(self):
        """Verifica quais leilões devem ser iniciados ou finalizados"""
        agora = datetime.datetime.now()
        
        for id_leilao, leilao in self.leiloes.items():
            # Verifica se deve iniciar
            if (leilao["status"] == "agendado" and 
                agora >= leilao["inicio"] and 
                agora < leilao["fim"]):
                self.iniciar_leilao(id_leilao)
            
            # Verifica se deve finalizar
            elif (leilao["status"] == "ativo" and 
                  agora >= leilao["fim"]):
                self.finalizar_leilao(id_leilao)
    
    def listar_leiloes(self):
        """Lista todos os leilões e seus status"""
        print("\n" + "="*60)
        print("📋 LEILÕES CADASTRADOS")
        print("="*60)
        
        for id_leilao, leilao in self.leiloes.items():
            status_emoji = {
                "agendado": "⏰",
                "ativo": "🔥",
                "finalizado": "✅"
            }
            
            print(f"{status_emoji.get(leilao['status'], '❓')} {id_leilao}")
            print(f"   📝 {leilao['descricao']}")
            print(f"   🕐 Início: {leilao['inicio'].strftime('%d/%m/%Y %H:%M')}")
            print(f"   🕐 Fim: {leilao['fim'].strftime('%d/%m/%Y %H:%M')}")
            print(f"   📊 Status: {leilao['status'].upper()}")
            print("-" * 60)
    
    def run(self):
        """Loop principal do MS Leilão"""
        print("\n🚀 MS Leilão iniciado! Monitorando leilões...")
        print("Pressione Ctrl+C para parar o serviço")
        
        try:
            while True:
                self.verificar_leiloes()
                time.sleep(5)  # Verifica a cada 5 segundos
                
        except KeyboardInterrupt:
            print("\n🛑 MS Leilão encerrado pelo usuário")
        except Exception as e:
            print(f"❌ Erro no MS Leilão: {e}")

if __name__ == '__main__':
    ms_leilao = MSLeilao()
    ms_leilao.listar_leiloes()
    ms_leilao.run()
