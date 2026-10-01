"""Serviço de Estoque: consome pedido.criado, reserva os itens e publica estoque.reservado."""
import logging
import random
import time

from mensageria import Consumidor

log = logging.getLogger("servico-estoque")


def reservar_itens(pedido: dict) -> None:
    time.sleep(random.uniform(0.2, 0.5))  # simula o acesso ao banco de estoque
    itens = ", ".join(f"{item['produtoId']} x{item['quantidade']}" for item in pedido["itens"])
    log.info("pedido %s: itens reservados (%s)", pedido["pedidoId"], itens)


if __name__ == "__main__":
    Consumidor(
        nome="servico-estoque",
        fila="estoque.reservar",
        processar=reservar_itens,
        evento_saida="estoque.reservado",
    ).iniciar()
