package httpapi_test

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"deepspace/internal/testdb"

	"github.com/jackc/pgx/v5/pgxpool"
)

// returnReq 发送归还请求。
func returnReq(t *testing.T, tsURL string, id int64, body any) (int, map[string]any) {
	t.Helper()
	return doJSON(t, http.MethodPost, tsURL+"/commands/"+strconv.FormatInt(id, 10)+"/return", body)
}

// claimReturn 领取并返回（id, generation, token）。
func claimReturn(t *testing.T, tsURL string) (int64, int64, string) {
	t.Helper()
	st, m := doJSON(t, http.MethodPost, tsURL+"/claims", map[string]any{"lease_duration_ms": 5000})
	if st != http.StatusOK {
		t.Fatalf("claim: status=%d body=%v", st, m)
	}
	return int64(m["command_id"].(float64)), int64(m["generation"].(float64)), m["lease_token"].(string)
}

// TestReturnValidation 归还入口的输入契约。
func TestReturnValidation(t *testing.T) {
	ts := newTestServer(t)
	id := createCmd(t, ts.URL, map[string]any{"x": 1}, nil)

	bad := []string{
		`{}`,
		`{"generation":1}`,
		`{"lease_token":"t"}`,
		`{"generation":"1","lease_token":"t"}`,
		`{"generation":1.5,"lease_token":"t"}`,
		`{"generation":0,"lease_token":"t"}`,
		`{"generation":true,"lease_token":"t"}`,
		`{"generation":1,"lease_token":123}`,
		`{"generation":1,"lease_token":""}`,
	}
	wantCode := []string{
		"missing_generation",
		"missing_lease_token",
		"missing_generation",
		"invalid_generation",
		"invalid_generation",
		"invalid_generation",
		"invalid_generation",
		"missing_lease_token",
		"missing_lease_token",
	}
	for i, body := range bad {
		st, raw := rawRequest(t, ts.URL, http.MethodPost,
			"/commands/"+strconv.FormatInt(id, 10)+"/return", body)
		if st != http.StatusUnprocessableEntity {
			t.Fatalf("body %q: status=%d want 422 (%s)", body, st, raw)
		}
		if got := decodeErrCode(raw); got != wantCode[i] {
			t.Fatalf("body %q: code=%s want %s", body, got, wantCode[i])
		}
	}

	// 非法 JSON -> 400。
	if st, _ := rawRequest(t, ts.URL, http.MethodPost,
		"/commands/"+strconv.FormatInt(id, 10)+"/return", "{oops"); st != http.StatusBadRequest {
		t.Fatalf("malformed return json: status=%d want 400", st)
	}
	// 未知/非数字编号 -> 404。
	if st, m := returnReq(t, ts.URL, 987654321, map[string]any{"generation": 1, "lease_token": "t"}); st != http.StatusNotFound ||
		m["error"].(map[string]any)["code"] != "unknown_command" {
		t.Fatalf("unknown return: status=%d body=%v want 404", st, m)
	}
	if st, raw := rawRequest(t, ts.URL, http.MethodPost, "/commands/abc/return",
		`{"generation":1,"lease_token":"t"}`); st != http.StatusNotFound ||
		decodeErrCode(raw) != "unknown_command" {
		t.Fatalf("non-numeric return id: status=%d raw=%s want 404", st, raw)
	}
}

// TestReturnHTTPHappyPath 端到端：领取 -> 归还（200，pending 无结算、leases[0]
// 有 returned_at）-> 立即重领更高代次 -> 旧令牌 ack 409、旧令牌归还幂等成功且
// 不清新租约 -> 新持有者送达 -> 唯一结算。
func TestReturnHTTPHappyPath(t *testing.T) {
	ts := newTestServer(t)
	id := createCmd(t, ts.URL, map[string]any{"ret": 1}, nil)

	cid, gen, token := claimReturn(t, ts.URL)
	if cid != id || gen != 1 {
		t.Fatalf("claim id=%d gen=%d want id=%d gen=1", cid, gen, id)
	}

	// 归还。
	st, m := returnReq(t, ts.URL, id, map[string]any{"generation": gen, "lease_token": token})
	if st != http.StatusOK {
		t.Fatalf("return: status=%d body=%v", st, m)
	}
	if m["status"] != "pending" {
		t.Fatalf("status after return=%v want pending", m["status"])
	}
	if m["settlement"] != nil {
		t.Fatalf("return must not settle: %v", m["settlement"])
	}
	leases := m["leases"].([]any)
	if len(leases) != 1 {
		t.Fatalf("want 1 lease row, got %d", len(leases))
	}
	if leases[0].(map[string]any)["returned_at"] == nil {
		t.Fatalf("returned_at must be recorded: %v", leases[0])
	}
	if m["current_lease_expires_at"] != nil {
		t.Fatalf("current lease pointer must be cleared: %v", m["current_lease_expires_at"])
	}
	if m["lease_generation"].(float64) != 1 {
		t.Fatalf("generation must remain 1: %v", m["lease_generation"])
	}

	// 立即重领：更高代次、新令牌。
	_, gen2, token2 := claimReturn(t, ts.URL)
	if gen2 != 2 || token2 == token {
		t.Fatalf("reclaim gen=%d token-changed=%v want gen=2 fresh token", gen2, token2 != token)
	}

	// 旧令牌 ack -> 409 lease_expired，状态不变。
	st, m = doJSON(t, http.MethodPost, ts.URL+"/commands/"+itoa(id)+"/ack",
		map[string]any{"lease_token": token, "result": "delivered"})
	if st != http.StatusConflict || m["error"].(map[string]any)["code"] != "lease_expired" {
		t.Fatalf("returned token ack: status=%d body=%v want 409 lease_expired", st, m)
	}

	// 旧令牌归还重放 -> 200 幂等，且不清新租约。
	st, m = returnReq(t, ts.URL, id, map[string]any{"generation": gen, "lease_token": token})
	if st != http.StatusOK {
		t.Fatalf("idempotent replay: status=%d body=%v", st, m)
	}
	if m["lease_generation"].(float64) != 2 || m["current_lease_expires_at"] == nil {
		t.Fatalf("replay must not clear successor lease: gen=%v exp=%v",
			m["lease_generation"], m["current_lease_expires_at"])
	}
	if m["leases"].([]any)[1].(map[string]any)["returned_at"] != nil {
		t.Fatalf("successor generation must not be marked returned")
	}

	// 新持有者送达。
	st, m = doJSON(t, http.MethodPost, ts.URL+"/commands/"+itoa(id)+"/ack",
		map[string]any{"lease_token": token2, "result": "delivered"})
	if st != http.StatusOK || m["status"] != "delivered" {
		t.Fatalf("gen-2 ack: status=%d body=%v", st, m)
	}
}

// TestReturnHTTPRejectionCodes 裁决拒绝路径的稳定错误码。
func TestReturnHTTPRejectionCodes(t *testing.T) {
	ts := newTestServer(t)

	// 陌生令牌 -> invalid_lease_token；代次不符 -> lease_expired。
	id := createCmd(t, ts.URL, map[string]any{"r": 1}, nil)
	_, gen, token := claimReturn(t, ts.URL)
	if st, m := returnReq(t, ts.URL, id, map[string]any{"generation": gen, "lease_token": "nope"}); st != http.StatusConflict ||
		m["error"].(map[string]any)["code"] != "invalid_lease_token" {
		t.Fatalf("unknown token: status=%d body=%v want 409 invalid_lease_token", st, m)
	}
	if st, m := returnReq(t, ts.URL, id, map[string]any{"generation": gen + 1, "lease_token": token}); st != http.StatusConflict ||
		m["error"].(map[string]any)["code"] != "lease_expired" {
		t.Fatalf("wrong generation: status=%d body=%v want 409 lease_expired", st, m)
	}

	// 已终结指令的首次归还（带一个陌生令牌）-> already_settled。
	if st, m := doJSON(t, http.MethodPost, ts.URL+"/commands/"+itoa(id)+"/ack",
		map[string]any{"lease_token": token, "result": "delivered"}); st != http.StatusOK {
		t.Fatalf("ack: status=%d body=%v", st, m)
	}
	if st, m := returnReq(t, ts.URL, id, map[string]any{"generation": gen, "lease_token": "nope"}); st != http.StatusConflict ||
		m["error"].(map[string]any)["code"] != "already_settled" {
		t.Fatalf("return settled: status=%d body=%v want 409 already_settled", st, m)
	}

	// 过期租约的归还 -> lease_expired。
	id2 := createCmd(t, ts.URL, map[string]any{"r2": 1}, nil)
	st0, m0 := doJSON(t, http.MethodPost, ts.URL+"/claims", map[string]any{"lease_duration_ms": 100})
	if st0 != http.StatusOK {
		t.Fatalf("claim2: %d %v", st0, m0)
	}
	g2 := int64(m0["generation"].(float64))
	t2 := m0["lease_token"].(string)
	time.Sleep(160 * time.Millisecond)
	if st, m := returnReq(t, ts.URL, id2, map[string]any{"generation": g2, "lease_token": t2}); st != http.StatusConflict ||
		m["error"].(map[string]any)["code"] != "lease_expired" {
		t.Fatalf("expired return: status=%d body=%v want 409 lease_expired", st, m)
	}
}

func itoa(id int64) string { return strconv.FormatInt(id, 10) }

// TestReturnAckConcurrentHTTPInterleave 真实 HTTP + 真实数据库交错：
// 同一有效租约上并发归还与回执，行锁裁决后只有一个结论——要么归还成功
// （仍 pending、无结算、可立即重领），要么送达（恰一行结算）；绝不两者并存。
func TestReturnAckConcurrentHTTPInterleave(t *testing.T) {
	for round := 0; round < 6; round++ {
		t.Run(fmt.Sprintf("round%d", round), func(t *testing.T) {
			pool := testdb.New(t)
			mustMigrate(t, pool)
			ts := newTestServerFromPool(t, pool)
			ctx := context.Background()

			id := createCmd(t, ts.URL, map[string]any{"round": round}, nil)
			cid, gen, token := claimReturn(t, ts.URL)
			if cid != id {
				t.Fatalf("claim got %d want %d", cid, id)
			}

			const writers = 8
			start := make(chan struct{})
			var wg sync.WaitGroup
			var mu sync.Mutex
			statuses := map[int]int{}
			wg.Add(writers)
			for i := 0; i < writers; i++ {
				go func(i int) {
					defer wg.Done()
					<-start
					var st int
					if i%2 == 0 {
						st, _ = returnReq(t, ts.URL, id,
							map[string]any{"generation": gen, "lease_token": token})
					} else {
						st, _ = doJSON(t, http.MethodPost, ts.URL+"/commands/"+itoa(id)+"/ack",
							map[string]any{"lease_token": token, "result": "delivered"})
					}
					mu.Lock()
					statuses[st]++
					mu.Unlock()
				}(i)
			}
			close(start)
			wg.Wait()

			var status string
			var settlements, returns int
			if err := pool.QueryRow(ctx, `SELECT status FROM commands WHERE id=$1`, id).Scan(&status); err != nil {
				t.Fatal(err)
			}
			if err := pool.QueryRow(ctx,
				`SELECT count(*) FROM settlements WHERE command_id=$1`, id).Scan(&settlements); err != nil {
				t.Fatal(err)
			}
			if err := pool.QueryRow(ctx,
				`SELECT count(*) FROM leases WHERE command_id=$1 AND returned_at IS NOT NULL`, id).Scan(&returns); err != nil {
				t.Fatal(err)
			}
			switch status {
			case "delivered":
				if settlements != 1 || returns != 0 || statuses[http.StatusOK] != 1 {
					t.Fatalf("delivered branch: statuses=%v settlements=%d returns=%d",
						statuses, settlements, returns)
				}
			case "pending":
				// 归还先落：4 个相同归还全部 200（1 真归还 + 3 幂等重放），4 个 ack 全 409。
				if settlements != 0 || returns != 1 || statuses[http.StatusOK] != 4 ||
					statuses[http.StatusConflict] != 4 {
					t.Fatalf("returned branch: statuses=%v settlements=%d returns=%d",
						statuses, settlements, returns)
				}
				// 归还后立即可重领，且为更高代次新令牌。
				_, gen2, token2 := claimReturn(t, ts.URL)
				if gen2 != gen+1 || token2 == token {
					t.Fatalf("reclaim after concurrent return: gen=%d token-same=%v want gen=%d fresh",
						gen2, token2 == token, gen+1)
				}
			default:
				t.Fatalf("unexpected status %s statuses=%v", status, statuses)
			}
		})
	}
}

// mustMigrate 对测试池执行迁移（供自建池的交错测试使用）。
func mustMigrate(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	if err := migratePool(context.Background(), pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
}
