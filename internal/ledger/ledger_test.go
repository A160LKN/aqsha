package ledger_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	ledgerv1 "github.com/A160LKN/aqsha/gen/aqsha/ledger/v1"
	"github.com/A160LKN/aqsha/internal/ledger"
	"github.com/A160LKN/aqsha/internal/platform"
	"github.com/A160LKN/aqsha/internal/testutil"
)

const (
	keyShop  = "key-shop-0001"
	keyOther = "key-other-0002"
)

func newClient(t *testing.T) ledgerv1.LedgerServiceClient {
	t.Helper()
	pool := testutil.DB(t, ledger.Migrations())
	svc := &ledger.Service{Pool: pool, Log: testutil.Log(t)}
	conn := testutil.GRPC(t, func(s *grpc.Server) { ledgerv1.RegisterLedgerServiceServer(s, svc) },
		platform.UnaryInterceptor(testutil.Log(t)),
		platform.AuthInterceptor(platform.ParseKeys(keyShop+":shop,"+keyOther+":other")))
	return ledgerv1.NewLedgerServiceClient(conn)
}

// as — контекст вызова от имени клиента с этим ключом.
func as(key string) context.Context {
	return metadata.AppendToOutgoingContext(context.Background(), platform.APIKeyHeader, key)
}

func TestOpenAndGetAccount(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	ctx := as(keyShop)

	opened, err := c.OpenAccount(ctx, &ledgerv1.OpenAccountRequest{Owner: "  Айбол  "})
	require.NoError(t, err)
	assert.Regexp(t, `^acc_[0-9a-f]{12}$`, opened.GetAccount().GetId())
	assert.Equal(t, "Айбол", opened.GetAccount().GetOwner(), "пробелы по краям обрезаются")
	assert.Zero(t, opened.GetAccount().GetBalanceTiyn())
	assert.Equal(t, "KZT", opened.GetAccount().GetCurrency())

	got, err := c.GetAccount(ctx, &ledgerv1.GetAccountRequest{Id: opened.GetAccount().GetId()})
	require.NoError(t, err)
	assert.Equal(t, opened.GetAccount().GetId(), got.GetAccount().GetId())
}

func TestOpenAccountValidation(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	for name, owner := range map[string]string{
		"пусто":          "",
		"только пробелы": "   ",
		"101 символ":     string(repeat('я', 101)),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := c.OpenAccount(as(keyShop), &ledgerv1.OpenAccountRequest{Owner: owner})
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
		})
	}
	// 100 символов — ещё можно
	_, err := c.OpenAccount(as(keyShop), &ledgerv1.OpenAccountRequest{Owner: string(repeat('я', 100))})
	assert.NoError(t, err)
}

func TestGetAccountErrors(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	_, err := c.GetAccount(as(keyShop), &ledgerv1.GetAccountRequest{Id: "acc_000000000000"})
	assert.Equal(t, codes.NotFound, status.Code(err))
	_, err = c.GetAccount(as(keyShop), &ledgerv1.GetAccountRequest{})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestDepositIsIdempotent(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	ctx := as(keyShop)
	acc := open(t, c)

	req := &ledgerv1.DepositRequest{AccountId: acc, AmountTiyn: 10_000_00, DepositId: "dep-1"}
	first, err := c.Deposit(ctx, req)
	require.NoError(t, err)
	assert.EqualValues(t, 10_000_00, first.GetAccount().GetBalanceTiyn())

	again, err := c.Deposit(ctx, req)
	require.NoError(t, err)
	assert.EqualValues(t, 10_000_00, again.GetAccount().GetBalanceTiyn(), "повтор с тем же deposit_id не зачисляет второй раз")

	_, err = c.Deposit(ctx, &ledgerv1.DepositRequest{AccountId: acc, AmountTiyn: 1, DepositId: "dep-1"})
	assert.Equal(t, codes.AlreadyExists, status.Code(err), "тот же deposit_id с другой суммой — ошибка, а не тихий повтор")
}

func TestDepositValidation(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	acc := open(t, c)
	cases := map[string]struct {
		req  *ledgerv1.DepositRequest
		code codes.Code
	}{
		"ноль":               {&ledgerv1.DepositRequest{AccountId: acc, AmountTiyn: 0, DepositId: "d"}, codes.InvalidArgument},
		"минус":              {&ledgerv1.DepositRequest{AccountId: acc, AmountTiyn: -5, DepositId: "d"}, codes.InvalidArgument},
		"без deposit_id":     {&ledgerv1.DepositRequest{AccountId: acc, AmountTiyn: 5}, codes.InvalidArgument},
		"deposit_id длиннее": {&ledgerv1.DepositRequest{AccountId: acc, AmountTiyn: 5, DepositId: string(repeat('x', 65))}, codes.InvalidArgument},
		"нет счёта":          {&ledgerv1.DepositRequest{AccountId: "acc_000000000000", AmountTiyn: 5, DepositId: "d"}, codes.NotFound},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := c.Deposit(as(keyShop), tc.req)
			assert.Equal(t, tc.code, status.Code(err))
		})
	}
}

// Без ключа сервис счетов ничего не делает: иначе любой в сети мог бы «напечатать» деньги через Deposit
func TestLedgerRequiresAPIKey(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	acc := open(t, c)
	for name, ctx := range map[string]context.Context{"без ключа": context.Background(), "неизвестный ключ": as("key-unknown-9999")} {
		t.Run(name, func(t *testing.T) {
			_, err := c.OpenAccount(ctx, &ledgerv1.OpenAccountRequest{Owner: "взломщик"})
			assert.Equal(t, codes.Unauthenticated, status.Code(err))
			_, err = c.Deposit(ctx, &ledgerv1.DepositRequest{AccountId: acc, AmountTiyn: 1_000_000_00, DepositId: "steal"})
			assert.Equal(t, codes.Unauthenticated, status.Code(err))
			_, err = c.GetAccount(ctx, &ledgerv1.GetAccountRequest{Id: acc})
			assert.Equal(t, codes.Unauthenticated, status.Code(err))
		})
	}
	got, err := c.GetAccount(as(keyShop), &ledgerv1.GetAccountRequest{Id: acc})
	require.NoError(t, err)
	assert.Zero(t, got.GetAccount().GetBalanceTiyn(), "деньги не появились")
}

// Чужой счёт для клиента не существует: ни прочитать баланс, ни пополнить
func TestForeignAccountIsInvisible(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	acc := open(t, c)
	_, err := c.GetAccount(as(keyOther), &ledgerv1.GetAccountRequest{Id: acc})
	assert.Equal(t, codes.NotFound, status.Code(err))
	_, err = c.Deposit(as(keyOther), &ledgerv1.DepositRequest{AccountId: acc, AmountTiyn: 5, DepositId: "dep-other"})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func open(t *testing.T, c ledgerv1.LedgerServiceClient) string {
	t.Helper()
	r, err := c.OpenAccount(as(keyShop), &ledgerv1.OpenAccountRequest{Owner: "тест"})
	require.NoError(t, err)
	return r.GetAccount().GetId()
}

func repeat(r rune, n int) []rune {
	out := make([]rune, n)
	for i := range out {
		out[i] = r
	}
	return out
}
