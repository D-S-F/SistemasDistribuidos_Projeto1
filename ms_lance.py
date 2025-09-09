import utils
import json
import sys, os

PUBLIC_KEYS_DIR = 'public_keys'

leiloes_ativos = set()
maiores_lances = {}
chaves_publicas_cache = {}


def get_public_key(user_id):

    if user_id in chaves_publicas_cache:
        return chaves_publicas_cache[user_id]
    

    key_path = os.path.join(PUBLIC_KEYS_DIR, f"{user_id}.pem")
    public_key = utils.load_public_key_from_file(key_path)
    
    if public_key:
        print(f"MS Lance: Chave pública para {user_id} carregada do arquivo e salva no cache.")
        chaves_publicas_cache[user_id] = public_key
    else:
        print(f"MS Lance: ATENÇÃO! Não foi possível encontrar a chave pública para {user_id} em {key_path}")

    return public_key

def callback_leilao_iniciado(ch, method, properties, body):
    if not body:
        return
    try:
        data = json.loads(body)
        id_leilao_iniciado = data['id_leilao']
        leiloes_ativos.add(id_leilao_iniciado)
        maiores_lances[id_leilao_iniciado] = {"id_usuario": None, "valor": 0}
        print(f"MS Lance: Leilão {id_leilao_iniciado} está ativo.")
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except json.JSONDecodeError:
        print(f" [!] Erro ao decodificar JSON: {body}")


def callback_leilao_finalizado(ch, method, properties, body):
    if not body:
        return
    data = json.loads(body)
    id_leilao_finalizado = data['id_leilao']
    if id_leilao_finalizado in leiloes_ativos:
        leiloes_ativos.remove(id_leilao_finalizado)
        
        vencedor = maiores_lances.get(id_leilao_finalizado)
        if vencedor and vencedor["id_usuario"]:
            publication = {
                "id_leilao": id_leilao_finalizado,
                "id_vencedor": vencedor["id_usuario"],
                "valor": vencedor["valor"]
            }
            channel = utils.get_rabbitmq_channel()
            channel.basic_publish(
                exchange='',
                routing_key='leilao_vencedor',
                body=json.dumps(publication)
            )
            print(f"MS Lance: Leilão {id_leilao_finalizado} encerrado. Vencedor: {vencedor['id_usuario']} com R${vencedor['valor']}.")
        else:
            print(f"MS Lance: Leilão {id_leilao_finalizado} encerrado sem lances.")
            
    ch.basic_ack(delivery_tag=method.delivery_tag)

def callback_lance_realizado(ch, method, properties, body):
    if not body:
        return
    data = json.loads(body)
    lance_info = data['data']
    id_usuario = lance_info['id_usuario']
    id_leilao_realizado = lance_info['id_leilao']
    valor_lance = lance_info['valor']
    ass = bytes.fromhex(data['assinatura'])

    public_key = get_public_key(id_usuario)
    if not public_key:
        print(f"Não foi possivel realizar a verificação do lance de {id_usuario}")
        return

    leilao_existe_e_ativo = id_leilao_realizado in leiloes_ativos
    lance_maior = valor_lance > maiores_lances.get(id_leilao_realizado, {}).get("valor", 0)

    ass_valida = utils.verify_signature(public_key, ass, lance_info)

    if leilao_existe_e_ativo and lance_maior and ass_valida:
        print(f"MS Lance: Lance de {id_usuario} no leilão {id_leilao_realizado} de R${valor_lance} é VÁLIDO.")
        maiores_lances[id_leilao_realizado] = {"id_usuario": id_usuario, "valor": valor_lance}
        
        channel = utils.get_rabbitmq_channel()
        channel.basic_publish(
            exchange='',
            routing_key='lance_validado',
            body=json.dumps(lance_info)
        )
    else:
        print(f"MS Lance: Lance de {id_usuario} no leilão {id_leilao_realizado} de R${valor_lance} é INVÁLIDO.")
        print(f"  - Leilão Ativo: {leilao_existe_e_ativo}")
        print(f"  - Lance Maior: {lance_maior} (Atual: {maiores_lances.get(id_leilao_realizado, {}).get('valor', 0)})")
        print(f"  - Assinatura válida: {ass_valida}")

    ch.basic_ack(delivery_tag=method.delivery_tag)

def run():
    channel = utils.get_rabbitmq_channel()
    utils.setup_queues(channel)
    

    result = channel.queue_declare(queue='', exclusive=True)
    queue_name_iniciado = result.method.queue
    channel.queue_bind(exchange='leilao_iniciado', queue=queue_name_iniciado)
    channel.basic_consume(queue=queue_name_iniciado, on_message_callback=callback_leilao_iniciado)
    channel.basic_consume(queue='leilao_finalizado', on_message_callback=callback_leilao_finalizado)
    channel.basic_consume(queue='lance_realizado', on_message_callback=callback_lance_realizado)
    
    print('MS Lance: Aguardando eventos...')
    channel.start_consuming()

if __name__ == '__main__':
    try:
        run()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)