using System.Text.Json.Serialization;
using ApiPedidos.Mensageria;
using ApiPedidos.Repositorio;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOptions<RabbitMqOptions>()
    .Bind(builder.Configuration.GetSection(RabbitMqOptions.Secao))
    .ValidateDataAnnotations()
    .ValidateOnStart();
builder.Services.AddSingleton<IPublicadorEventos, RabbitMqPublicador>();
builder.Services.AddSingleton<IPedidoRepositorio, PedidoRepositorioEmMemoria>();
builder.Services.AddControllers()
    .AddJsonOptions(o => o.JsonSerializerOptions.Converters.Add(new JsonStringEnumConverter()));

var app = builder.Build();

app.MapControllers();
app.MapGet("/health", () => Results.Ok(new { status = "ok" }));

app.Run();
