// Serviço de Logística: consome pagamento.aprovado e agenda a entrega do pedido.
//
// Para cada mensagem recebida:
//   - JSON inválido ou erro fatal   -> nack(requeue=false) -> DLX -> DLQ
//   - mensagem repetida (messageId) -> ack e ignora
//   - falha técnica                 -> republica em ecommerce.retry (volta em 5 s);
//     na última tentativa vai para a DLQ
//   - sucesso                       -> ack
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	nome             = "servico-logistica"
	fila             = "logistica.agendar"
	exchangeRetry    = "ecommerce.retry"
	headerTentativas = "x-tentativas"
)

var (
	maxTentativas   = envInt("MAX_TENTATIVAS", 3)
	falhaPercentual = envInt("FALHA_PERCENTUAL", 0)
	falhaFatal      = envInt("FALHA_FATAL_PERCENTUAL", 0)
	prefetch        = envInt("PREFETCH", 10)

	log = slog.New(slog.NewTextHandler(os.Stdout, nil)).With("servico", nome)

	// errFatal: não adianta tentar de novo, a mensagem vai direto para a DLQ.
	errFatal = errors.New("erro fatal")
)

type Pedido struct {
	PedidoID   string  `json:"pedidoId"`
	ClienteID  string  `json:"clienteId"`
	Itens      []Item  `json:"itens"`
	ValorTotal float64 `json:"valorTotal"`
	MessageID  string  `json:"messageId"`
	Timestamp  string  `json:"timestamp"`
}

type Item struct {
	ProdutoID     string  `json:"produtoId"`
	Quantidade    int     `json:"quantidade"`
	PrecoUnitario float64 `json:"precoUnitario"`
}

func lerMensagem(body []byte) (Pedido, error) {
	var p Pedido
	if err := json.Unmarshal(body, &p); err != nil {
		return p, fmt.Errorf("%w: JSON inválido: %v", errFatal, err)
	}
	if p.PedidoID == "" || p.ClienteID == "" || p.MessageID == "" || p.Timestamp == "" || len(p.Itens) == 0 {
		return p, fmt.Errorf("%w: mensagem sem os campos obrigatórios", errFatal)
	}
	return p, nil
}

// simularFalha sorteia um erro para mostrar o retry e a DLQ funcionando.
func simularFalha() error {
	sorteio := rand.IntN(100)
	switch {
	case sorteio < falhaFatal:
		return fmt.Errorf("%w simulado", errFatal)
	case sorteio < falhaFatal+falhaPercentual:
		return errors.New("falha técnica simulada")
	}
	return nil
}

func agendarEntrega(p Pedido) {
	time.Sleep(time.Duration(200+rand.IntN(300)) * time.Millisecond) // simula a transportadora
	rastreio := fmt.Sprintf("BR%09d", rand.IntN(1_000_000_000))
	previsao := time.Now().AddDate(0, 0, 3).Format("02/01/2006")
	log.Info("entrega agendada", "pedidoId", p.PedidoID, "rastreio", rastreio, "previsao", previsao)
}

type consumidor struct {
	canal       *amqp.Channel
	processadas map[string]bool // messageIds já processados (idempotência)
}

func (c *consumidor) aoReceber(d amqp.Delivery) {
	pedido, err := lerMensagem(d.Body)
	if err != nil {
		log.Error("mensagem inválida -> DLQ", "erro", err)
		d.Nack(false, false)
		return
	}

	if c.processadas[pedido.MessageID] {
		log.Info("mensagem repetida, ignorando", "pedidoId", pedido.PedidoID)
		d.Ack(false)
		return
	}

	tentativa := lerTentativas(d.Headers) + 1
	if err := simularFalha(); err != nil {
		switch {
		case errors.Is(err, errFatal):
			log.Error("erro fatal -> DLQ", "pedidoId", pedido.PedidoID, "erro", err)
			d.Nack(false, false)
		case tentativa >= maxTentativas:
			log.Error("falhou todas as tentativas -> DLQ", "pedidoId", pedido.PedidoID, "tentativas", tentativa)
			d.Nack(false, false)
		default:
			log.Warn("falha, vai tentar de novo", "pedidoId", pedido.PedidoID,
				"tentativa", tentativa, "max", maxTentativas, "erro", err)
			if err := c.republicarNoRetry(d, tentativa); err != nil {
				log.Error("não foi possível agendar o retry -> DLQ", "pedidoId", pedido.PedidoID, "erro", err)
				d.Nack(false, false)
				return
			}
			d.Ack(false)
		}
		return
	}

	agendarEntrega(pedido)
	c.processadas[pedido.MessageID] = true
	d.Ack(false)
}

// republicarNoRetry manda a mensagem para a fila de espera. Quando o TTL de 5 s
// expira, o RabbitMQ devolve a mensagem para ecommerce.eventos com a routing key original.
// Usa context.Background() para que um SIGTERM não interrompa a publicação no meio.
func (c *consumidor) republicarNoRetry(d amqp.Delivery, tentativa int) error {
	confirmacao, err := c.canal.PublishWithDeferredConfirmWithContext(context.Background(), exchangeRetry, d.RoutingKey, true, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		MessageId:    d.MessageId,
		AppId:        nome,
		Headers:      amqp.Table{headerTentativas: int32(tentativa)},
		Body:         d.Body,
	})
	if err != nil {
		return err
	}
	if !confirmacao.Wait() {
		return errors.New("o broker não confirmou a publicação")
	}
	return nil
}

// lerTentativas lê o header x-tentativas (int32 ou int64, depende de quem publicou).
func lerTentativas(h amqp.Table) int {
	switch v := h[headerTentativas].(type) {
	case int32:
		return int(v)
	case int64:
		return int(v)
	}
	return 0
}

func consumir(ctx context.Context, amqpURL string) error {
	hostname, _ := os.Hostname()
	conexao, err := amqp.DialConfig(amqpURL, amqp.Config{
		Heartbeat: 30 * time.Second,
		// nome@container: identifica cada réplica no painel do RabbitMQ
		Properties: amqp.Table{"connection_name": nome + "@" + hostname},
	})
	if err != nil {
		return err
	}
	defer conexao.Close()

	// Ao receber SIGTERM fecha a conexão; as mensagens sem ack voltam para a fila.
	go func() {
		<-ctx.Done()
		conexao.Close()
	}()

	canal, err := conexao.Channel()
	if err != nil {
		return err
	}
	if err := canal.Confirm(false); err != nil { // publisher confirms
		return err
	}
	if err := canal.Qos(prefetch, 0, false); err != nil { // divide a carga entre réplicas
		return err
	}
	entregas, err := canal.Consume(fila, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	log.Info("consumindo a fila", "fila", fila)

	c := &consumidor{canal: canal, processadas: map[string]bool{}}
	for d := range entregas {
		c.aoReceber(d)
	}
	return errors.New("canal fechado")
}

func main() {
	amqpURL := (&url.URL{
		Scheme: "amqp",
		User:   url.UserPassword(envObrigatoria("RABBITMQ_USER"), envObrigatoria("RABBITMQ_PASS")),
		Host:   envObrigatoria("RABBITMQ_HOST") + ":5672",
		Path:   "/" + envObrigatoria("RABBITMQ_VHOST"),
	}).String()

	ctx, parar := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer parar()

	for {
		err := consumir(ctx, amqpURL)
		if ctx.Err() != nil {
			log.Info("encerrando")
			return
		}
		log.Warn("conexão com o RabbitMQ perdida, tentando de novo em 3 s", "erro", err)
		time.Sleep(3 * time.Second)
	}
}

func envObrigatoria(chave string) string {
	valor := os.Getenv(chave)
	if valor == "" {
		log.Error("variável de ambiente obrigatória ausente", "variavel", chave)
		os.Exit(1)
	}
	return valor
}

func envInt(chave string, padrao int) int {
	if valor, err := strconv.Atoi(os.Getenv(chave)); err == nil {
		return valor
	}
	return padrao
}
