// Package transfer описывает статусы перевода и разрешённые переходы между ними.
package transfer

// Status — статус перевода в сервисе платежей.
type Status string

const (
	Pending   Status = "pending"
	Completed Status = "completed"
	Failed    Status = "failed"
)

// CanTransition говорит, можно ли перейти из одного статуса в другой.
// Итоговые статусы конечны: выполненный перевод нельзя «провалить» повторным событием.
func CanTransition(from, to Status) bool {
	return from == Pending && (to == Completed || to == Failed)
}

// Final — итоговый ли статус.
func (s Status) Final() bool { return s == Completed || s == Failed }
