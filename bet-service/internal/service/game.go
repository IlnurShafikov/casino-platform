package service

import "math/rand"

// Game определяет исход одного раунда для конкретного типа игры.
// Реализуется каждым поддерживаемым GameType и регистрируется в мапе,
// с которой создаётся BetService — чтобы добавить новую игру, нужно
// написать новый тип, реализующий этот интерфейс, и зарегистрировать его.
type Game interface {
	// Play разыгрывает один раунд на ставку amount и возвращает исход:
	// выиграл ли игрок, и если да — сумму выигрыша.
	Play(amount int64) (won bool, winAmount int64)
}

// SlotGame — простая слот-машина с фиксированной таблицей множителей:
// из 9 исходов 5 — проигрыш, остальные — выигрыш с множителем от 1x до
// 4x ставки. Заглушка для настоящей игровой механики со своим RTP.
type SlotGame struct{}

func (SlotGame) Play(amount int64) (bool, int64) {
	multipliers := []int64{0, 1, 0, 2, 0, 3, 0, 4, 0}

	idx := rand.Int63n(int64(len(multipliers) - 1))
	winMulti := multipliers[idx]

	if winMulti == 0 {
		return false, 0
	}

	return true, amount * winMulti
}
