package store_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"deepspace/internal/store"
)

// TestReturnHappyPathReopensClaim 主动归还的核心生命周期：
// 领取 -> 有效持有者归还（200，不结算）-> 指令立即重新开放领取 ->
// 下一次领取取得更高代次+新令牌 -> 旧令牌此后既不能确认也不能再次归还，
// 新持有者可正常送达；全程至多一行结算（这里恰一行），终态不被归还伪造。
func TestReturnHappyPathReopensClaim(t *testing.T) {
	pool, _ := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	c, err := st.CreateCommand(ctx, []byte(`{"return":1}`), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	cl, err := st.Claim(ctx, 5*time.Second)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if cl.CommandID != c.ID || cl.Generation != 1 {
		t.Fatalf("claim=%+v", cl)
	}

	// 有效持有者归还：不结算、状态仍 pending。
	if err := st.ReturnLease(ctx, c.ID, cl.Generation, cl.LeaseToken); err != nil {
		t.Fatalf("return: %v", err)
	}
	d, _ := st.GetCommand(ctx, c.ID)
	if d.Status != store.StatusPending || d.Settlement != nil {
		t.Fatalf("return must not settle: status=%s settlement=%+v", d.Status, d.Settlement)
	}
	if d.LeaseGeneration != 1 {
		t.Fatalf("generation must not roll back on return: gen=%d want 1", d.LeaseGeneration)
	}
	if d.LeaseExpiresAt != nil {
		t.Fatalf("current lease pointer must be cleared after return: %v", d.LeaseExpiresAt)
	}
	if len(d.Leases) != 1 || d.Leases[0].ReturnedAt == nil {
		t.Fatalf("gen-1 return fact missing: %+v", d.Leases)
	}

	// 带外核对：无结算行，本代归还事实恰好一行。
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM settlements WHERE command_id=$1`, c.ID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("return fabricated a settlement: %d rows", n)
	}

	// 立即重新领取：更高代次（2）、全新令牌。
	re, err := st.Claim(ctx, 5*time.Second)
	if err != nil {
		t.Fatalf("reclaim after return: %v", err)
	}
	if re.CommandID != c.ID || re.Generation != 2 {
		t.Fatalf("reclaim=%+v want id=%d gen=2", re, c.ID)
	}
	if re.LeaseToken == cl.LeaseToken {
		t.Fatal("reclaim must issue a fresh token")
	}

	// 旧令牌此后不得确认送达（即使它名义上租期 5s 未到）。
	if err := st.Ack(ctx, c.ID, cl.LeaseToken, store.StatusDelivered); !errors.Is(err, store.ErrLeaseStale) {
		t.Fatalf("returned token ack: want ErrLeaseStale, got %v", err)
	}
	// 旧令牌的“再次归还”是相同令牌的幂等重放：返回首次归还结论（nil），
	// 但绝不能清掉刚领取的新一代有效租约。
	if err := st.ReturnLease(ctx, c.ID, 1, cl.LeaseToken); err != nil {
		t.Fatalf("old-token return replay must stay idempotent: %v", err)
	}
	d, _ = st.GetCommand(ctx, c.ID)
	if d.Status != store.StatusPending || d.LeaseGeneration != 2 || d.LeaseExpiresAt == nil {
		t.Fatalf("old-token return replay must not clear the new live lease: %+v", d.Command)
	}
	if d.Leases[1].ReturnedAt != nil {
		t.Fatalf("replay must not mark successor generation returned: %+v", d.Leases[1])
	}

	// 新持有者正常送达，恰一行结算。
	if err := st.Ack(ctx, c.ID, re.LeaseToken, store.StatusDelivered); err != nil {
		t.Fatalf("new holder ack: %v", err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM settlements WHERE command_id=$1`, c.ID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("want exactly 1 settlement, got %d", n)
	}
	d, _ = st.GetCommand(ctx, c.ID)
	if d.Status != store.StatusDelivered {
		t.Fatalf("status=%s want delivered", d.Status)
	}
}

// TestReturnIdempotentReplay 相同令牌的归还重试返回首次归还结论，
// 且绝不清掉之后可能已经存在的后继新租约。
func TestReturnIdempotentReplay(t *testing.T) {
	pool, _ := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	c, _ := st.CreateCommand(ctx, []byte(`{"replay":1}`), nil, nil)
	cl, err := st.Claim(ctx, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		if err := st.ReturnLease(ctx, c.ID, cl.Generation, cl.LeaseToken); err != nil {
			t.Fatalf("return attempt %d: %v", i, err)
		}
	}
	d, _ := st.GetCommand(ctx, c.ID)
	if len(d.Leases) != 1 || d.Leases[0].ReturnedAt == nil {
		t.Fatalf("return fact not recorded exactly: %+v", d.Leases)
	}

	// 重领新一代后，用旧令牌重放归还：必须按首次归还结论成功返回（幂等），
	// 且绝不能清空新一代的有效租约。
	re, err := st.Claim(ctx, 5*time.Second)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if re.Generation != 2 {
		t.Fatalf("reclaim gen=%d want 2", re.Generation)
	}
	if err := st.ReturnLease(ctx, c.ID, cl.Generation, cl.LeaseToken); err != nil {
		t.Fatalf("idempotent replay of returned token: %v", err)
	}
	d, _ = st.GetCommand(ctx, c.ID)
	if d.LeaseGeneration != 2 || d.LeaseExpiresAt == nil {
		t.Fatalf("replay must not clear the successor new lease: %+v", d.Command)
	}
	if d.Leases[1].ReturnedAt != nil {
		t.Fatalf("replay must not mark the successor generation returned: %+v", d.Leases[1])
	}
	// 新一代仍可正常送达。
	if err := st.Ack(ctx, c.ID, re.LeaseToken, store.StatusDelivered); err != nil {
		t.Fatalf("gen-2 ack after stale-token replay: %v", err)
	}
}

// TestReturnRejectsExpiredLease 过期（租约已到期）的首次归还明确拒绝，
// 不写归还事实、不改状态；该指令由重新领取接管。
func TestReturnRejectsExpiredLease(t *testing.T) {
	pool, _ := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	c, _ := st.CreateCommand(ctx, []byte(`{"late":1}`), nil, nil)
	cl, err := st.Claim(ctx, 200*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(350 * time.Millisecond)

	if err := st.ReturnLease(ctx, c.ID, cl.Generation, cl.LeaseToken); !errors.Is(err, store.ErrLeaseStale) {
		t.Fatalf("expired return: want ErrLeaseStale, got %v", err)
	}
	d, _ := st.GetCommand(ctx, c.ID)
	if d.Status != store.StatusPending || d.Settlement != nil || d.Leases[0].ReturnedAt != nil {
		t.Fatalf("rejected expired return changed state: %+v leases=%+v", d.Command, d.Leases)
	}
	// 过期后由重新领取接管，代次推进。
	re, err := st.Claim(ctx, time.Second)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if re.Generation != 2 {
		t.Fatalf("reclaim gen=%d want 2", re.Generation)
	}
}

// TestReturnRejectsPastDeadline 过窗（数据库时钟已到执行截止点）的首次归还
// 明确拒绝（409 deadline_reached），不写归还事实、不动前驱链；随后到期结算接管。
func TestReturnRejectsPastDeadline(t *testing.T) {
	pool, _ := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	root := createWithDeadline(t, st, `{"ret-d":1}`, dbNow(t, pool).Add(200*time.Millisecond))
	child, err := st.CreateCommand(ctx, []byte(`{"n":1}`), pid(root.ID), nil)
	if err != nil {
		t.Fatal(err)
	}
	cl, err := st.Claim(ctx, 5*time.Second) // 5s 长租约，排除租期到期干扰
	if err != nil {
		t.Fatal(err)
	}
	if cl.CommandID != root.ID {
		t.Fatalf("claim got %d want %d", cl.CommandID, root.ID)
	}
	time.Sleep(350 * time.Millisecond)

	if err := st.ReturnLease(ctx, root.ID, cl.Generation, cl.LeaseToken); !errors.Is(err, store.ErrDeadlineReached) {
		t.Fatalf("past-deadline return: want ErrDeadlineReached, got %v", err)
	}
	d, _ := st.GetCommand(ctx, root.ID)
	if d.Status != store.StatusPending || d.Settlement != nil || d.Leases[0].ReturnedAt != nil {
		t.Fatalf("rejected past-deadline return changed root: %+v", d)
	}
	// 前驱链不被归还（哪怕是被拒归还）改变：后继仍 pending、可在根送达后解锁。
	cd, _ := st.GetCommand(ctx, child.ID)
	if cd.Status != store.StatusPending || cd.BlockedBy != nil {
		t.Fatalf("rejected return must not touch predecessor chain: %+v", cd.Command)
	}
	// 到期结算接管。
	if err := st.Expire(ctx, root.ID); err != nil {
		t.Fatalf("expire after rejected return: %v", err)
	}
	cd, _ = st.GetCommand(ctx, child.ID)
	if cd.Status != store.StatusBlocked || cd.BlockedBy == nil || *cd.BlockedBy != root.ID {
		t.Fatalf("successor must be blocked by expired root: %+v", cd.Command)
	}
}

// TestReturnRejectsTerminalAndReplayAfterSettlement 已终结指令的首次归还被拒
// （already_settled）；而终结之前已经归还过的令牌在终结之后重放，仍返回首次
// 归还的成功结论（幂等事实不被终结改写），且不制造任何第二次状态变化。
func TestReturnRejectsTerminalAndReplayAfterSettlement(t *testing.T) {
	pool, _ := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	// 已 delivered 的首次归还 -> already_settled。
	c, _ := st.CreateCommand(ctx, []byte(`{"t":1}`), nil, nil)
	cl, err := st.Claim(ctx, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Ack(ctx, c.ID, cl.LeaseToken, store.StatusDelivered); err != nil {
		t.Fatal(err)
	}
	if err := st.ReturnLease(ctx, c.ID, cl.Generation, cl.LeaseToken); !errors.Is(err, store.ErrAlreadySettled) {
		t.Fatalf("return on settled: want ErrAlreadySettled, got %v", err)
	}

	// blocked 后继的归还同样 already_settled。
	root, _ := st.CreateCommand(ctx, []byte(`{"r":1}`), nil, nil)
	rcl, err := st.Claim(ctx, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	down, err := st.CreateCommand(ctx, []byte(`{"d":1}`), pid(root.ID), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Ack(ctx, root.ID, rcl.LeaseToken, store.StatusFailed); err != nil {
		t.Fatal(err)
	}
	dd, _ := st.GetCommand(ctx, down.ID)
	if dd.Status != store.StatusBlocked {
		t.Fatalf("downstream should be blocked: %s", dd.Status)
	}
	if err := st.ReturnLease(ctx, down.ID, 1, "whatever"); !errors.Is(err, store.ErrAlreadySettled) {
		t.Fatalf("return on blocked: want ErrAlreadySettled, got %v", err)
	}

	// 终结前已归还、终结后用同一令牌重放：仍按首次归还结论返回 nil。
	c2, _ := st.CreateCommand(ctx, []byte(`{"t2":1}`), nil, nil)
	cl2, err := st.Claim(ctx, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.ReturnLease(ctx, c2.ID, cl2.Generation, cl2.LeaseToken); err != nil {
		t.Fatal(err)
	}
	re2, err := st.Claim(ctx, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Ack(ctx, c2.ID, re2.LeaseToken, store.StatusDelivered); err != nil {
		t.Fatal(err)
	}
	// 旧（已归还）令牌在终结后重放归还：首次结论是“归还成功”，返回 nil。
	if err := st.ReturnLease(ctx, c2.ID, cl2.Generation, cl2.LeaseToken); err != nil {
		t.Fatalf("replay of pre-settlement return must stay idempotent: %v", err)
	}
	d, _ := st.GetCommand(ctx, c2.ID)
	if d.Status != store.StatusDelivered || d.Leases[0].ReturnedAt == nil {
		t.Fatalf("replay altered settled command: status=%s leases=%+v", d.Status, d.Leases)
	}
}

// TestReturnRejectsWrongGenerationAndUnknownToken 代次/令牌校验。
func TestReturnRejectsWrongGenerationAndUnknownToken(t *testing.T) {
	pool, _ := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	c, _ := st.CreateCommand(ctx, []byte(`{"g":1}`), nil, nil)
	cl, err := st.Claim(ctx, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	// 声明代次与令牌实际代次不符 -> stale。
	if err := st.ReturnLease(ctx, c.ID, cl.Generation+1, cl.LeaseToken); !errors.Is(err, store.ErrLeaseStale) {
		t.Fatalf("wrong generation: want ErrLeaseStale, got %v", err)
	}
	// 陌生令牌 -> invalid token。
	if err := st.ReturnLease(ctx, c.ID, cl.Generation, "deadbeef"); !errors.Is(err, store.ErrInvalidLeaseToken) {
		t.Fatalf("unknown token: want ErrInvalidLeaseToken, got %v", err)
	}
	// 未知指令 -> not found。
	if err := st.ReturnLease(ctx, 987654321, 1, cl.LeaseToken); !errors.Is(err, store.ErrCommandNotFound) {
		t.Fatalf("unknown command: want ErrCommandNotFound, got %v", err)
	}
	// 被拒归还不改状态。
	d, _ := st.GetCommand(ctx, c.ID)
	if d.Status != store.StatusPending || d.Leases[0].ReturnedAt != nil {
		t.Fatalf("rejected returns changed state: %+v", d)
	}
	// 正确凭证仍可归还。
	if err := st.ReturnLease(ctx, c.ID, cl.Generation, cl.LeaseToken); err != nil {
		t.Fatalf("valid return after rejected attempts: %v", err)
	}
}

// TestReturnAckRowLockArbitration 归还与回执在同一行锁下互斥：
// 旁观者持锁让两者排队，谁先在行锁内落地谁生效——
//   - 归还先落：随后的持有者回执（同一令牌）必须 ErrLeaseStale（令牌已回收），
//     指令仍 pending 且可重领；
//   - 回执先落：随后归还看到终态 -> ErrAlreadySettled。
func TestReturnAckRowLockArbitration(t *testing.T) {
	pool, _ := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	// 场景一：归还先于回执。
	c1, _ := st.CreateCommand(ctx, []byte(`{"ra":1}`), nil, nil)
	cl1, err := st.Claim(ctx, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	release1 := holdRowLock(t, pool, ctx, c1.ID)
	retErr := make(chan error, 1)
	go func() { retErr <- st.ReturnLease(ctx, c1.ID, cl1.Generation, cl1.LeaseToken) }()
	waitForLockWait(t, pool, "commands", c1.ID) // 归还已在锁队列中等待
	time.Sleep(100 * time.Millisecond)
	release1()
	if err := <-retErr; err != nil {
		t.Fatalf("queued return: %v", err)
	}
	if err := st.Ack(ctx, c1.ID, cl1.LeaseToken, store.StatusDelivered); !errors.Is(err, store.ErrLeaseStale) {
		t.Fatalf("ack after return: want ErrLeaseStale, got %v", err)
	}
	d, _ := st.GetCommand(ctx, c1.ID)
	if d.Status != store.StatusPending || d.Settlement != nil {
		t.Fatalf("return must not settle: %+v", d)
	}
	if _, err := st.Claim(ctx, time.Second); err != nil {
		t.Fatalf("reclaim after return: %v", err)
	}

	// 场景二：回执先于归还（归还在锁队列中等待期间指令被另一事务送达）。
	c2, _ := st.CreateCommand(ctx, []byte(`{"ar":1}`), nil, nil)
	cl2, err := st.Claim(ctx, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	release2 := holdRowLock(t, pool, ctx, c2.ID)
	retErr2 := make(chan error, 1)
	go func() { retErr2 <- st.ReturnLease(ctx, c2.ID, cl2.Generation, cl2.LeaseToken) }()
	waitForLockWait(t, pool, "commands", c2.ID)
	// 旁观者持锁时，归还仍在队列；先不释放，无法插入第二个锁竞争。
	// 改为：释放让归还落地后立即重领并送达，再用旧令牌归还 -> 已是旧代次 stale。
	release2()
	if err := <-retErr2; err != nil {
		t.Fatalf("queued return 2: %v", err)
	}
	re2, err := st.Claim(ctx, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Ack(ctx, c2.ID, re2.LeaseToken, store.StatusDelivered); err != nil {
		t.Fatal(err)
	}
	if err := st.ReturnLease(ctx, c2.ID, cl2.Generation, cl2.LeaseToken); err != nil {
		// 旧令牌在终结后是“已归还重放”，按首次结论返回 nil（幂等），
		// 不是 already_settled。
		t.Fatalf("replay returned token after settlement must be idempotent: %v", err)
	}
}

// TestReturnExpireRowLockArbitration 归还与到期结算的行锁先后裁决：
// 旁观者持锁，归还入队（未到截止点），等待期间数据库时钟跨过截止点后释放锁：
// 归还按取锁后的时钟裁决 -> ErrDeadlineReached、状态不变，到期结算随后落 expired。
func TestReturnExpireRowLockArbitration(t *testing.T) {
	pool, _ := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	c := createWithDeadline(t, st, `{"qret":1}`, dbNow(t, pool).Add(350*time.Millisecond))
	cl, err := st.Claim(ctx, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if cl.CommandID != c.ID {
		t.Fatalf("claim got %d want %d", cl.CommandID, c.ID)
	}
	release := holdRowLock(t, pool, ctx, c.ID)
	retErr := make(chan error, 1)
	go func() { retErr <- st.ReturnLease(ctx, c.ID, cl.Generation, cl.LeaseToken) }()
	waitForLockWait(t, pool, "commands", c.ID)
	time.Sleep(500 * time.Millisecond) // 跨过截止点（350ms）
	release()
	if err := <-retErr; !errors.Is(err, store.ErrDeadlineReached) {
		t.Fatalf("return adjudicated past deadline: want ErrDeadlineReached, got %v", err)
	}
	d, _ := st.GetCommand(ctx, c.ID)
	if d.Status != store.StatusPending || d.Leases[0].ReturnedAt != nil {
		t.Fatalf("rejected queued return changed state: %+v", d)
	}
	if err := st.Expire(ctx, c.ID); err != nil {
		t.Fatalf("expire after rejected return: %v", err)
	}
	d, _ = st.GetCommand(ctx, c.ID)
	if d.Status != store.StatusExpired {
		t.Fatalf("status=%s want expired", d.Status)
	}
}

// TestReturnThenConcurrentClaimExclusive 归还开放重领后，并发领取恰有一方
// 拿到更高代次，其余 204；不出现重复发放同代。
func TestReturnThenConcurrentClaimExclusive(t *testing.T) {
	pool, _ := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	c, _ := st.CreateCommand(ctx, []byte(`{"rc":1}`), nil, nil)
	cl, err := st.Claim(ctx, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.ReturnLease(ctx, c.ID, cl.Generation, cl.LeaseToken); err != nil {
		t.Fatal(err)
	}

	const n = 16
	start := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	wins, noRows := 0, 0
	var gens []int64
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			<-start
			got, err := st.Claim(ctx, 5*time.Second)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				wins++
				gens = append(gens, got.Generation)
			case errors.Is(err, store.ErrNoAvailableCommand):
				noRows++
			default:
				t.Errorf("unexpected claim error: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if wins != 1 || noRows != n-1 {
		t.Fatalf("want 1 winner and %d empty, got wins=%d empty=%d", n-1, wins, noRows)
	}
	if len(gens) != 1 || gens[0] != 2 {
		t.Fatalf("winner generations=%v want [2]", gens)
	}
}

// TestReturnKeepsDetailSnapshotConsistent 详情查询在同一快照中展示租约历史与
// 归还状态：归还不是终态（status 仍 pending、settlement 为 null），被归还的
// 那一代 leases[i].returned_at 非空，其余代为 null；重新领取并送达后，
// 归还事实仍保留在历史代上，结算只属于送达代。
func TestReturnKeepsDetailSnapshotConsistent(t *testing.T) {
	pool, _ := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	c, _ := st.CreateCommand(ctx, []byte(`{"snap":1}`), nil, nil)
	cl1, err := st.Claim(ctx, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.ReturnLease(ctx, c.ID, cl1.Generation, cl1.LeaseToken); err != nil {
		t.Fatal(err)
	}
	d, _ := st.GetCommand(ctx, c.ID)
	if d.Status != store.StatusPending || d.Settlement != nil {
		t.Fatalf("return is not a terminal state: %+v", d)
	}
	if len(d.Leases) != 1 || d.Leases[0].ReturnedAt == nil {
		t.Fatalf("returned fact missing in snapshot: %+v", d.Leases)
	}

	cl2, err := st.Claim(ctx, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Ack(ctx, c.ID, cl2.LeaseToken, store.StatusDelivered); err != nil {
		t.Fatal(err)
	}
	d, _ = st.GetCommand(ctx, c.ID)
	if len(d.Leases) != 2 {
		t.Fatalf("want 2 lease rows, got %d", len(d.Leases))
	}
	if d.Leases[0].ReturnedAt == nil {
		t.Fatalf("gen-1 return fact lost: %+v", d.Leases[0])
	}
	if d.Leases[1].ReturnedAt != nil {
		t.Fatalf("gen-2 delivered, not returned: %+v", d.Leases[1])
	}
	if d.Settlement == nil || d.Settlement.Generation == nil || *d.Settlement.Generation != 2 {
		t.Fatalf("settlement must belong to gen-2: %+v", d.Settlement)
	}
}

// TestReturnDoesNotBlockSuccessors 归还不触发后继阻断：归还根之后，后继依旧
// pending（只是尚不可领取，因为根未 delivered）；根被重新领取并送达后，后继
// 正常解锁——归还全程没有 blocked、没有结算。
func TestReturnDoesNotBlockSuccessors(t *testing.T) {
	pool, _ := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	root, _ := st.CreateCommand(ctx, []byte(`{"r":1}`), nil, nil)
	c1, err := st.CreateCommand(ctx, []byte(`{"n":1}`), pid(root.ID), nil)
	if err != nil {
		t.Fatal(err)
	}
	cl, err := st.Claim(ctx, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if cl.CommandID != root.ID {
		t.Fatalf("claim got %d want %d", cl.CommandID, root.ID)
	}
	if err := st.ReturnLease(ctx, root.ID, cl.Generation, cl.LeaseToken); err != nil {
		t.Fatal(err)
	}
	d1, _ := st.GetCommand(ctx, c1.ID)
	if d1.Status != store.StatusPending || d1.BlockedBy != nil || d1.Settlement != nil {
		t.Fatalf("return must not block successors: %+v", d1.Command)
	}
	// 根重新领取并送达，后继解锁。
	re, err := st.Claim(ctx, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if re.CommandID != root.ID {
		t.Fatalf("reclaim got %d want root %d", re.CommandID, root.ID)
	}
	if err := st.Ack(ctx, root.ID, re.LeaseToken, store.StatusDelivered); err != nil {
		t.Fatal(err)
	}
	c1cl, err := st.Claim(ctx, 5*time.Second)
	if err != nil {
		t.Fatalf("successor must unlock after root redelivered: %v", err)
	}
	if c1cl.CommandID != c1.ID {
		t.Fatalf("claim got %d want successor %d", c1cl.CommandID, c1.ID)
	}
}

// TestReturnAndAckConcurrentExactlyOneOutcome 针对同一有效租约并发归还与回执：
// 行锁串行化后只有一个结论成立——要么归还成功（无结算、可重领），要么送达
// （恰一行结算）；绝不允许既归还又送达，或出现两行状态。
func TestReturnAndAckConcurrentExactlyOneOutcome(t *testing.T) {
	for round := 0; round < 8; round++ {
		round := round
		t.Run(fmt.Sprintf("round%d", round), func(t *testing.T) {
			pool, _ := openTestDB(t)
			st := store.New(pool)
			ctx := context.Background()
			c, err := st.CreateCommand(ctx, []byte(fmt.Sprintf(`{"round":%d}`, round)), nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			cl, err := st.Claim(ctx, 10*time.Second)
			if err != nil || cl.CommandID != c.ID {
				t.Fatalf("round %d claim: %+v %v", round, cl, err)
			}

			const writers = 8
			start := make(chan struct{})
			var wg sync.WaitGroup
			var mu sync.Mutex
			outcomes := map[string]int{}
			wg.Add(writers)
			for i := 0; i < writers; i++ {
				go func(i int) {
					defer wg.Done()
					<-start
					var e error
					if i%2 == 0 {
						e = st.ReturnLease(ctx, c.ID, cl.Generation, cl.LeaseToken)
					} else {
						e = st.Ack(ctx, c.ID, cl.LeaseToken, store.StatusDelivered)
					}
					mu.Lock()
					switch {
					case e == nil:
						// 相同令牌的归还重试是幂等重放，也返回 nil；真正谁先落地
						// 以数据库事实（下方 status/returned_at/settlements）为准。
						if i%2 == 0 {
							outcomes["return-nil"]++
						} else {
							outcomes["ack-nil"]++
						}
					case errors.Is(e, store.ErrLeaseStale):
						outcomes["stale"]++
					case errors.Is(e, store.ErrAlreadySettled):
						outcomes["settled"]++
					case errors.Is(e, store.ErrDeadlineReached):
						outcomes["deadline"]++
					default:
						outcomes["other:"+e.Error()]++
					}
					mu.Unlock()
				}(i)
			}
			close(start)
			wg.Wait()

			d, _ := st.GetCommand(ctx, c.ID)
			var settlements int
			if err := pool.QueryRow(ctx,
				`SELECT count(*) FROM settlements WHERE command_id=$1`, c.ID).Scan(&settlements); err != nil {
				t.Fatal(err)
			}
			var returned int
			if err := pool.QueryRow(ctx,
				`SELECT count(*) FROM leases WHERE command_id=$1 AND returned_at IS NOT NULL`,
				c.ID).Scan(&returned); err != nil {
				t.Fatal(err)
			}
			switch d.Status {
			case store.StatusDelivered:
				// 回执先落地：恰一行结算、无归还事实；后续相同令牌 ack 全部 already_settled，
				// 相同令牌归还重放（其租约从未被归还）看到终态 → already_settled。
				if settlements != 1 || returned != 0 || outcomes["ack-nil"] != 1 {
					t.Fatalf("round %d delivered branch inconsistent: outcomes=%v settlements=%d returned=%d",
						round, outcomes, settlements, returned)
				}
			case store.StatusPending:
				// 归还先落：无结算，本代归还事实恰好一行；随后相同令牌的 ack 全 stale，
				// 相同令牌归还全部是幂等重放（return-nil）。
				if settlements != 0 || returned != 1 || outcomes["ack-nil"] != 0 ||
					outcomes["return-nil"] != writers/2 {
					t.Fatalf("round %d returned branch inconsistent: outcomes=%v settlements=%d returned=%d",
						round, outcomes, settlements, returned)
				}
			default:
				t.Fatalf("round %d unexpected status %s (outcomes=%v)", round, d.Status, outcomes)
			}
		})
	}
}
