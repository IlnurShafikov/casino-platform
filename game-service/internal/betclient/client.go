// Package betclient содержит HTTP-клиент к bet-service — game-service
// не хранит ставки сам, а вызывает уже готовый API bet-service.
package betclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// StatusPending — статус ставки сразу после создания, до того как
// bet-service асинхронно разыграет её исход через Kafka.
const StatusPending = "PENDING"

type Bet struct {
	ID        int64     `json:"id"`
	UserID    int64     `json:"user_id"`
	Amount    int64     `json:"amount"`
	GameType  string    `json:"game_type"`
	Status    string    `json:"status"`
	WinAmount int64     `json:"win_amount"`
	CreatedAt time.Time `json:"created_at"`
}

// UpstreamError оборачивает ошибочный HTTP-ответ от bet-service, сохраняя
// исходный статус-код и сообщение — чтобы вызывающий мог вернуть игроку
// тот же статус, а не свернуть всё до общего 502.
type UpstreamError struct {
	StatusCode int
	Message    string
}

func (e *UpstreamError) Error() string {
	return fmt.Sprintf("bet-service returned %d: %s", e.StatusCode, e.Message)
}

type PlaceBetRequest struct {
	Amount   int64  `json:"amount"`
	GameType string `json:"game_type"`
}

// Client описывает вызовы к bet-service, нужные game-service.
type Client interface {
	// PlaceBet вызывает POST /bets в bet-service. authHeader — значение
	// заголовка Authorization целиком (со схемой "Bearer "), пробрасываемое
	// от игрока как есть: bet-service сам достаёт user_id из токена.
	PlaceBet(ctx context.Context, authHeader string, req PlaceBetRequest) (*Bet, error)
	// GetBet вызывает GET /bets/{id}.
	GetBet(ctx context.Context, authHeader string, betID int64) (*Bet, error)
}

// client — реализация Client поверх стандартного http.Client.
type client struct {
	baseURL    string
	httpClient *http.Client
}

// NewClient создаёт Client, обращающийся к bet-service по baseURL.
// httpClient.Timeout — подстраховка на случай, если вызывающий не задаст
// свой дедлайн через ctx; основной контроль времени ожидания делается
// через ctx на стороне сервиса (там, где происходит поллинг GetBet).
func NewClient(baseURL string) Client {
	return &client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

func (c *client) PlaceBet(
	ctx context.Context,
	authHeader string,
	req PlaceBetRequest,
) (*Bet, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(
		ctx, http.MethodPost, c.baseURL+"/bets", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", authHeader)

	return c.do(httpReq)
}

func (c *client) GetBet(
	ctx context.Context,
	authHeader string,
	betID int64,
) (*Bet, error) {
	httpReq, err := http.NewRequestWithContext(
		ctx, http.MethodGet, fmt.Sprintf("%s/bets/%d", c.baseURL, betID), nil,
	)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	httpReq.Header.Set("Authorization", authHeader)

	return c.do(httpReq)
}

// do выполняет запрос и декодирует JSON-ответ в Bet. HTTP-статус >= 400
// превращается в ошибку с текстом из тела ответа ({"error": "..."} —
// формат, единый для всех сервисов проекта).
func (c *client) do(req *http.Request) (*Bet, error) {
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("call bet-service: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		var errBody struct {
			Error string `json:"error"`
		}

		_ = json.NewDecoder(resp.Body).Decode(&errBody)

		return nil, &UpstreamError{StatusCode: resp.StatusCode, Message: errBody.Error}
	}

	var bet Bet

	if err := json.NewDecoder(resp.Body).Decode(&bet); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return &bet, nil
}
