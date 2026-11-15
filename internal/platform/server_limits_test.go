package platform_test

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/A160LKN/aqsha/internal/platform"
)

// Огромное сообщение отклоняется до обработки: память сервиса защищена от «бомбы» в запросе.
func TestGRPCServerRejectsOversizedMessages(t *testing.T) {
	lis := bufconn.Listen(1 << 20)
	srv := platform.NewGRPCServer()
	healthpb.RegisterHealthServer(srv, health.NewServer())
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	client := healthpb.NewHealthClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = client.Check(ctx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err, "обычный запрос проходит")

	_, err = client.Check(ctx, &healthpb.HealthCheckRequest{Service: strings.Repeat("x", platform.MaxRecvMsgSize+1)})
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
}
