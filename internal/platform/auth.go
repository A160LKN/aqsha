package platform

import (
	"context"
	"crypto/subtle"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// APIKeyHeader — метаданные gRPC с ключом клиента.
const APIKeyHeader = "x-api-key"

// ClientHeader — заголовок Kafka с клиентом, от имени которого создано событие.
// Сервис счетов по нему проверяет, что клиент списывает деньги со своего счёта.
const ClientHeader = "x-client-id"

// publicMethods — служебные вызовы без ключа: пробы Kubernetes и reflection для grpcurl.
// Всё остальное закрыто по умолчанию: новый сервис или метод не окажется открытым по забывчивости.
var publicMethods = []string{"/grpc.health.v1.", "/grpc.reflection."}

type clientKey struct{}

// WithClient кладёт клиента в контекст. Нужен тестам и обработчикам событий Kafka.
func WithClient(ctx context.Context, client string) context.Context {
	return context.WithValue(ctx, clientKey{}, client)
}

// ClientID — клиент, которому принадлежит запрос; пустая строка, если запрос без авторизации.
func ClientID(ctx context.Context) string {
	id, _ := ctx.Value(clientKey{}).(string)
	return id
}

// ParseKeys разбирает переменную вида "ключ1:клиент1,ключ2:клиент2".
func ParseKeys(s string) map[string]string {
	keys := map[string]string{}
	for _, pair := range strings.Split(s, ",") {
		k, c, ok := strings.Cut(strings.TrimSpace(pair), ":")
		if ok && k != "" && c != "" {
			keys[k] = c
		}
	}
	return keys
}

// AuthInterceptor пускает только запросы с известным x-api-key и кладёт клиента в контекст.
func AuthInterceptor(keys map[string]string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		for _, prefix := range publicMethods {
			if strings.HasPrefix(info.FullMethod, prefix) {
				return handler(ctx, req)
			}
		}
		var given string
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if v := md.Get(APIKeyHeader); len(v) > 0 {
				given = v[0]
			}
		}
		for key, client := range keys {
			// Сравнение за постоянное время: по времени ответа ключ не подобрать
			if subtle.ConstantTimeCompare([]byte(key), []byte(given)) == 1 {
				return handler(WithClient(ctx, client), req)
			}
		}
		return nil, status.Error(codes.Unauthenticated, "нужен действующий x-api-key")
	}
}
