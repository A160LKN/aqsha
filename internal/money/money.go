// Package money проверяет суммы. Деньги везде — целые тиыны (1 ₸ = 100 тиынов).
package money

import "fmt"

// MaxTiyn — потолок одной операции: 1 млрд ₸. Выше — почти наверняка ошибка ввода,
// а запас до предела int64 исключает переполнение при сложении балансов.
const MaxTiyn int64 = 1_000_000_000 * 100

// Validate возвращает ошибку для суммы, которую нельзя провести.
func Validate(tiyn int64) error {
	switch {
	case tiyn <= 0:
		return fmt.Errorf("сумма должна быть больше нуля, получено %d", tiyn)
	case tiyn > MaxTiyn:
		return fmt.Errorf("сумма больше допустимой: %d > %d тиынов", tiyn, MaxTiyn)
	}
	return nil
}
