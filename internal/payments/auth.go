package payments

import (
	"context"

	"google.golang.org/grpc"

	"github.com/A160LKN/aqsha/internal/platform"
)

// APIKeyHeader — метаданные с ключом клиента.
const APIKeyHeader = platform.APIKeyHeader

// ClientID — клиент, которому принадлежит запрос.
func ClientID(ctx context.Context) string { return platform.ClientID(ctx) }

// ParseKeys разбирает API_KEYS вида "ключ1:клиент1,ключ2:клиент2".
func ParseKeys(s string) map[string]string { return platform.ParseKeys(s) }

// AuthInterceptor пускает только запросы с известным x-api-key. Проверки здоровья и reflection — без ключа.
func AuthInterceptor(keys map[string]string) grpc.UnaryServerInterceptor {
	return platform.AuthInterceptor(keys)
}
