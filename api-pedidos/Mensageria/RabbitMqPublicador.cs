using System.ComponentModel.DataAnnotations;
using System.Text.Json;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace ApiPedidos.Mensageria;

/// <summary>
/// Conexão com o broker. Vem das variáveis de ambiente RabbitMq__Host, RabbitMq__Usuario etc.
/// Se faltar alguma, a API não sobe (ValidateOnStart no Program.cs).
/// </summary>
public class RabbitMqOptions
{
    public const string Secao = "RabbitMq";

    [Required] public string Host { get; set; } = "";
    [Required] public string VirtualHost { get; set; } = "";
    [Required] public string Usuario { get; set; } = "";
    [Required] public string Senha { get; set; } = "";
    [Required] public string Exchange { get; set; } = "";
}

public interface IPublicadorEventos
{
    Task PublicarAsync<T>(string routingKey, T evento, string messageId, CancellationToken ct = default);
}

/// <summary>
/// Publica mensagens persistentes na exchange central com publisher confirms:
/// PublicarAsync só termina depois que o broker confirma que gravou a mensagem.
/// </summary>
public sealed class RabbitMqPublicador : IPublicadorEventos, IAsyncDisposable
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    private readonly RabbitMqOptions _opcoes;
    private readonly ConnectionFactory _factory;
    private readonly SemaphoreSlim _lock = new(1, 1); // um canal, uma publicação por vez
    private IConnection? _conexao;
    private IChannel? _canal;

    public RabbitMqPublicador(IOptions<RabbitMqOptions> opcoes)
    {
        _opcoes = opcoes.Value;
        _factory = new ConnectionFactory
        {
            HostName = _opcoes.Host,
            VirtualHost = _opcoes.VirtualHost,
            UserName = _opcoes.Usuario,
            Password = _opcoes.Senha,
            ClientProvidedName = $"api-pedidos@{Environment.MachineName}",
            // A reconexão é feita por nós: se algo falhar, fechamos tudo e
            // reconectamos na próxima publicação.
            AutomaticRecoveryEnabled = false
        };
    }

    public async Task PublicarAsync<T>(string routingKey, T evento, string messageId, CancellationToken ct = default)
    {
        var corpo = JsonSerializer.SerializeToUtf8Bytes(evento, Json);
        var propriedades = new BasicProperties
        {
            ContentType = "application/json",
            DeliveryMode = DeliveryModes.Persistent,
            MessageId = messageId,
            AppId = "api-pedidos"
        };

        await _lock.WaitAsync(ct);
        try
        {
            var canal = await ObterCanalAsync(ct);
            // mandatory: se nenhuma fila receber a mensagem, dá erro em vez de ela sumir.
            await canal.BasicPublishAsync(_opcoes.Exchange, routingKey, mandatory: true, propriedades, corpo, ct);
        }
        catch
        {
            await FecharAsync();
            throw;
        }
        finally
        {
            _lock.Release();
        }
    }

    private async Task<IChannel> ObterCanalAsync(CancellationToken ct)
    {
        if (_conexao is { IsOpen: true } && _canal is { IsOpen: true })
            return _canal;

        await FecharAsync();
        _conexao = await _factory.CreateConnectionAsync(ct);
        _canal = await _conexao.CreateChannelAsync(
            new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true), ct);
        return _canal;
    }

    private async Task FecharAsync()
    {
        try
        {
            if (_canal is not null) await _canal.DisposeAsync();
            if (_conexao is not null) await _conexao.DisposeAsync();
        }
        catch
        {
            // a conexão já caiu; não há o que fechar
        }
        _canal = null;
        _conexao = null;
    }

    public async ValueTask DisposeAsync() => await FecharAsync();
}
