-- 0004_returns.sql
-- 主动归还（relinquish）：中继领取上行指令后发现设备暂不可用（失联、未就位等），
-- 需要主动归还本代租约，让仍在执行窗口内的指令重新排队，而不是伪造 failed 结算。
--
-- 设计要点：
--   * leases.returned_at 记录本代租约“被其当前有效持有者主动归还”的事实
--     （取锁后的数据库裁决时刻 clock_timestamp()）；NULL 表示该代租约未被归还。
--     归还不是终态：commands.status 仍为 pending，settlements 不新增任何行，
--     也不阻断任何后继——归还只开放重新领取；
--   * 每次成功归还同时清空 commands 上指向该代租约的有效凭证
--     （lease_token/lease_expires_at 置 NULL），但 lease_generation 保持不变：
--     下一次领取必然取得更高代次（lease_generation+1）与全新令牌，
--     被归还的旧令牌此后既不能确认送达（Ack 侧以 returned_at 拒绝），
--     也不能再次归还（重放只读到首次归还结论，绝不清掉后继新租约）；
--   * 幂等键就是租约自身：lease_token 全局唯一，且 (command_id, generation) 唯一，
--     相同令牌的归还重试命中 leases.returned_at 非空即直接返回首次归还结论，
--     无需额外的归还表；该唯一事实绝不被重试清写，更不会触碰后继代次的新租约；
--   * 只有“仍 pending + 当前代次 + 租约未到期 + 数据库时钟仍在执行截止点之前”
--     的有效持有者才能完成首次归还。过期（租约已到期）、过窗（已到 deadline_at）
--     或指令已终结（delivered/failed/blocked/expired）的首次归还一律拒绝，
--     且不写 returned_at、不改前驱链、不产生结算。
--
-- 所有裁决与 Ack/Expire/Claim 在同一把 commands 行锁下串行，裁决时刻同样是
-- 拿到行锁之后读取的数据库真实时钟（clock_timestamp()）：归还、回执、到期结算、
-- 重新领取的先后完全由数据库按取锁后的时钟裁决，实例本地时钟不参与。

ALTER TABLE leases
    ADD COLUMN IF NOT EXISTS returned_at TIMESTAMPTZ NULL;
