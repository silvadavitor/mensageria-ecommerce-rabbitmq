"""Serviço de Pagamento: consome estoque.reservado, processa a cobrança e publica pagamento.aprovado."""
import logging
import random
import time
import uuid

from mensageria import Consumidor

log = logging.getLogger("servico-pagamento")


def processar_cobranca(pedido: dict) -> None:
    time.sleep(random.uniform(0.3, 0.8))  # simula a chamada ao gateway de pagamento
    transacao = uuid.uuid4().hex[:12]
    log.info("pedido %s: cobrança de R$ %.2f aprovada (transação %s)",
             pedido["pedidoId"], float(pedido["valorTotal"]), transacao)


if __name__ == "__main__":
    Consumidor(
        nome="servico-pagamento",
        fila="pagamento.processar",
        processar=processar_cobranca,
        evento_saida="pagamento.aprovado",
    ).iniciar()
