using ApiPedidos.Mensageria;
using ApiPedidos.Models;
using ApiPedidos.Repositorio;
using Microsoft.AspNetCore.Mvc;

namespace ApiPedidos.Controllers;

[ApiController]
[Route("pedidos")]
public class PedidosController(
    IPedidoRepositorio repositorio,
    IPublicadorEventos publicador,
    ILogger<PedidosController> logger) : ControllerBase
{
    private const string RoutingKeyPedidoCriado = "pedido.criado";

    /// <summary>Registra o pedido, publica pedido.criado e responde 202 sem esperar o processamento.</summary>
    [HttpPost]
    public async Task<IActionResult> Criar([FromBody] CriarPedidoRequest request)
    {
        if (request.Itens.Any(item => item is null))
        {
            ModelState.AddModelError(nameof(request.Itens), "A lista de itens não pode ter itens nulos.");
            return ValidationProblem(ModelState);
        }

        var agora = DateTimeOffset.UtcNow;
        var pedido = new Pedido
        {
            PedidoId = Guid.NewGuid().ToString(),
            ClienteId = request.ClienteId,
            Itens = request.Itens,
            ValorTotal = request.Itens.Sum(i => i.Quantidade * i.PrecoUnitario),
            CriadoEm = agora
        };
        repositorio.Salvar(pedido);

        var evento = new PedidoCriadoEvento(
            pedido.PedidoId, pedido.ClienteId, pedido.Itens, pedido.ValorTotal,
            MessageId: Guid.NewGuid().ToString(), Timestamp: agora);

        try
        {
            await publicador.PublicarAsync(RoutingKeyPedidoCriado, evento, evento.MessageId);
        }
        catch (Exception ex)
        {
            pedido.Status = StatusPedido.FalhaAoPublicar;
            logger.LogError(ex, "Falha ao publicar {RoutingKey} do pedido {PedidoId}", RoutingKeyPedidoCriado, pedido.PedidoId);
            return Problem(
                title: "Broker de mensagens indisponível",
                detail: "O pedido não pôde ser enviado para processamento. Tente novamente.",
                statusCode: StatusCodes.Status503ServiceUnavailable);
        }

        logger.LogInformation("Pedido {PedidoId} recebido (R$ {Valor}); {RoutingKey} publicado",
            pedido.PedidoId, pedido.ValorTotal, RoutingKeyPedidoCriado);

        return AcceptedAtAction(nameof(Obter), new { pedidoId = pedido.PedidoId },
            new { pedido.PedidoId, status = pedido.Status.ToString(), pedido.ValorTotal });
    }

    [HttpGet("{pedidoId}")]
    public IActionResult Obter(string pedidoId)
    {
        var pedido = repositorio.Obter(pedidoId);
        return pedido is null ? NotFound() : Ok(pedido);
    }
}
