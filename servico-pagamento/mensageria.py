"""Consumidor RabbitMQ usado pelos serviços Python (Estoque e Pagamento).

Para cada mensagem recebida:
- JSON inválido ou erro fatal     -> nack(requeue=False) -> DLX -> DLQ
- mensagem repetida (messageId)   -> ack e ignora
- falha técnica                   -> republica em ecommerce.retry (volta em 5 s);
                                     na última tentativa vai para a DLQ
- sucesso                         -> publica o próximo evento e só então dá ack
"""
import json
import logging
import os
import random
import signal
import socket
import time
import uuid
from datetime import datetime, timezone

import pika
from pika.exceptions import AMQPError

EXCHANGE_EVENTOS = "ecommerce.eventos"
EXCHANGE_RETRY = "ecommerce.retry"
HEADER_TENTATIVAS = "x-tentativas"
CAMPOS_OBRIGATORIOS = ("pedidoId", "clienteId", "itens", "valorTotal", "messageId", "timestamp")

MAX_TENTATIVAS = int(os.getenv("MAX_TENTATIVAS", "3"))
FALHA_PERCENTUAL = float(os.getenv("FALHA_PERCENTUAL", "0"))
FALHA_FATAL_PERCENTUAL = float(os.getenv("FALHA_FATAL_PERCENTUAL", "0"))
PREFETCH = int(os.getenv("PREFETCH", "10"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
logging.getLogger("pika").setLevel(logging.WARNING)


class ErroFatal(Exception):
    """Erro que não adianta tentar de novo: a mensagem vai direto para a DLQ."""


def env_obrigatoria(nome: str) -> str:
    valor = os.getenv(nome, "").strip()
    if not valor:
        raise SystemExit(f"variável de ambiente obrigatória ausente: {nome}")
    return valor


def simular_falha() -> None:
    """Sorteia um erro para mostrar o retry e a DLQ funcionando."""
    sorteio = random.uniform(0, 100)
    if sorteio < FALHA_FATAL_PERCENTUAL:
        raise ErroFatal("erro fatal simulado")
    if sorteio < FALHA_FATAL_PERCENTUAL + FALHA_PERCENTUAL:
        raise RuntimeError("falha técnica simulada")


def ler_mensagem(body: bytes) -> dict:
    try:
        msg = json.loads(body)
    except ValueError as e:
        raise ErroFatal(f"JSON inválido: {e}") from e
    if not isinstance(msg, dict) or any(campo not in msg for campo in CAMPOS_OBRIGATORIOS):
        raise ErroFatal("mensagem sem os campos obrigatórios")
    if not isinstance(msg["itens"], list) or not msg["itens"]:
        raise ErroFatal("itens deve ser uma lista não vazia")
    return msg


def ler_tentativas(props) -> int:
    """Quantas vezes a mensagem já falhou (header x-tentativas; 0 se ausente ou inválido)."""
    valor = (props.headers or {}).get(HEADER_TENTATIVAS, 0)
    return valor if isinstance(valor, int) else 0


def novo_evento(pedido: dict, evento: str) -> dict:
    """Mesmo pedido, nova mensagem.

    O messageId é derivado de pedidoId + evento: se esta etapa for refeita (ex.: o
    container caiu depois de publicar e antes do ack), sai o mesmo messageId e o
    próximo serviço reconhece a mensagem como repetida.
    """
    return {
        "pedidoId": pedido["pedidoId"],
        "clienteId": pedido["clienteId"],
        "itens": pedido["itens"],
        "valorTotal": pedido["valorTotal"],
        "messageId": str(uuid.uuid5(uuid.NAMESPACE_URL, f"{pedido['pedidoId']}/{evento}")),
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
    }


def parar(*_) -> None:
    # Dentro do container o processo é o PID 1 e ignora SIGTERM sem handler.
    raise KeyboardInterrupt


class Consumidor:
    def __init__(self, nome: str, fila: str, processar, evento_saida: str):
        self.nome = nome
        self.fila = fila
        self.processar = processar
        self.evento_saida = evento_saida
        self.log = logging.getLogger(nome)
        self.processadas: set[str] = set()  # messageIds já processados (idempotência)

    def iniciar(self) -> None:
        parametros = pika.ConnectionParameters(
            host=env_obrigatoria("RABBITMQ_HOST"),
            virtual_host=env_obrigatoria("RABBITMQ_VHOST"),
            credentials=pika.PlainCredentials(env_obrigatoria("RABBITMQ_USER"), env_obrigatoria("RABBITMQ_PASS")),
            heartbeat=30,
            # nome@container: identifica cada réplica no painel do RabbitMQ
            client_properties={"connection_name": f"{self.nome}@{socket.gethostname()}"},
        )
        signal.signal(signal.SIGTERM, parar)

        while True:
            try:
                conexao = pika.BlockingConnection(parametros)
                canal = conexao.channel()
                canal.confirm_delivery()                 # publisher confirms
                canal.basic_qos(prefetch_count=PREFETCH)  # divide a carga entre réplicas
                canal.basic_consume(queue=self.fila, on_message_callback=self.ao_receber)
                self.log.info("consumindo a fila '%s'", self.fila)
                canal.start_consuming()
            except AMQPError as e:
                self.log.warning("conexão com o RabbitMQ perdida (%s), tentando de novo em 3 s", e)
                time.sleep(3)
            except KeyboardInterrupt:
                # Mensagens sem ack voltam para a fila e outra réplica processa.
                self.log.info("encerrando")
                return

    def publicar(self, canal, exchange: str, routing_key: str, body: bytes, message_id: str, headers=None) -> None:
        propriedades = pika.BasicProperties(
            content_type="application/json",
            delivery_mode=pika.DeliveryMode.Persistent,
            message_id=message_id,
            app_id=self.nome,
            headers=headers,
        )
        # mandatory=True: dá erro se nenhuma fila receber a mensagem, em vez de ela sumir
        canal.basic_publish(exchange, routing_key, body, propriedades, mandatory=True)

    def ao_receber(self, canal, method, props, body: bytes) -> None:
        tag = method.delivery_tag
        try:
            msg = ler_mensagem(body)
        except ErroFatal as e:
            self.log.error("mensagem inválida -> DLQ: %s", e)
            canal.basic_nack(tag, requeue=False)
            return

        pedido_id = msg["pedidoId"]
        if msg["messageId"] in self.processadas:
            self.log.info("pedido %s: mensagem repetida, ignorando", pedido_id)
            canal.basic_ack(tag)
            return

        tentativa = ler_tentativas(props) + 1
        try:
            simular_falha()
            self.processar(msg)
            evento = novo_evento(msg, self.evento_saida)
            self.publicar(canal, EXCHANGE_EVENTOS, self.evento_saida, json.dumps(evento).encode(), evento["messageId"])
            self.log.info("pedido %s: publicado '%s'", pedido_id, self.evento_saida)
            self.processadas.add(msg["messageId"])
            canal.basic_ack(tag)

        except ErroFatal as e:
            self.log.error("pedido %s: %s -> DLQ", pedido_id, e)
            canal.basic_nack(tag, requeue=False)

        except Exception as e:
            if tentativa >= MAX_TENTATIVAS:
                self.log.error("pedido %s: falhou %d vezes -> DLQ", pedido_id, tentativa)
                canal.basic_nack(tag, requeue=False)
                return
            self.log.warning("pedido %s: falha na tentativa %d/%d (%s), vai tentar de novo",
                             pedido_id, tentativa, MAX_TENTATIVAS, e)
            self.publicar(canal, EXCHANGE_RETRY, method.routing_key, body, msg["messageId"],
                          headers={HEADER_TENTATIVAS: tentativa})
            canal.basic_ack(tag)
