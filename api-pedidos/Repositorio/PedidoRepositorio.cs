using System.Collections.Concurrent;
using ApiPedidos.Models;

namespace ApiPedidos.Repositorio;

public interface IPedidoRepositorio
{
    void Salvar(Pedido pedido);
    Pedido? Obter(string pedidoId);
}

/// <summary>Persistência em memória, suficiente para a simulação.</summary>
public class PedidoRepositorioEmMemoria : IPedidoRepositorio
{
    private readonly ConcurrentDictionary<string, Pedido> _pedidos = new();

    public void Salvar(Pedido pedido) => _pedidos[pedido.PedidoId] = pedido;

    public Pedido? Obter(string pedidoId) => _pedidos.GetValueOrDefault(pedidoId);
}
