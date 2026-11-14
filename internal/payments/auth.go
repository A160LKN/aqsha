package payments

import (
	"context"
	"crypto/subtle"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// APIKeyHeader — метаданные с ключом клиента.
const APIKeyHeader = "x-api-key"

type clientKey struct{}

// ClientID — клиент, которому принадлежит запрос.
func ClientID(ctx context.Context) string {
	id, _ := ctx.Value(clientKey{}).(string)
	return id
}

// ParseKeys разбирает API_KEYS вида "ключ1:клиент1,ключ2:клиент2".
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

// AuthInterceptor пускает только запросы с известным x-api-key. Проверки здоровья и reflection — без ключа.
func AuthInterceptor(keys map[string]string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if !strings.HasPrefix(info.FullMethod, "/aqsha.payments.") {
			return handler(ctx, req)
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
				return handler(context.WithValue(ctx, clientKey{}, client), req)
			}
		}
		return nil, status.Error(codes.Unauthenticated, "нужен действующий x-api-key")
	}
}
