using System.ComponentModel.DataAnnotations;

namespace ApiPedidos.Models;

public record ItemPedido(
    [Required] string ProdutoId,
    [Range(1, 10_000)] int Quantidade,
    [Range(0.01, 1_000_000)] decimal PrecoUnitario);

public record CriarPedidoRequest(
    [Required] string ClienteId,
    [Required, MinLength(1), MaxLength(100)] List<ItemPedido> Itens);

public enum StatusPedido
{
    Recebido,
    FalhaAoPublicar
}

public class Pedido
{
    public required string PedidoId { get; init; }
    public required string ClienteId { get; init; }
    public required List<ItemPedido> Itens { get; init; }
    public required decimal ValorTotal { get; init; }
    public required DateTimeOffset CriadoEm { get; init; }
    public StatusPedido Status { get; set; } = StatusPedido.Recebido;
}

/// <summary>Formato da mensagem trafegada no broker (seção 4.2 do documento).</summary>
public record PedidoCriadoEvento(
    string PedidoId,
    string ClienteId,
    List<ItemPedido> Itens,
    decimal ValorTotal,
    string MessageId,
    DateTimeOffset Timestamp);
