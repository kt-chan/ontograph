#!/usr/bin/env python3
"""
seed.py — 反欺诈交易流样例数据（可复现）

产出：
  - 5 家银行
  - 200 个账户（id 1..200，均匀分布到 5 家银行）
  - 2000 笔正常背景交易
  - 3 类可疑模式（structuring / rapid_in_out / impossible_travel），共 14 笔
  - 恰好 14 条 fraud_assessments（只打给可疑簇，不误伤正常交易）
  - 按 (bank, date) 聚合的 clearing_batches / clearing_records
     —— 每笔交易对发送方银行记 OUT、接收方银行记 IN

可疑交易使用 INSERT ... RETURNING id 精确捕获，避免"发送方 id 相同"误伤。
可在任意环境重复执行（开头会 TRUNCATE ... RESTART IDENTITY CASCADE）。

用法：
    python ./scripts/seed.py
"""

from __future__ import annotations

import os
import random
import sys
from datetime import datetime, timedelta, timezone

import psycopg2
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv(usecwd=True))

# ---------------- 常量 ----------------
N_BANKS = 5
N_ACCOUNTS = 200
N_NORMAL_TX = 2000
ACCT_ID_MAX = N_ACCOUNTS

BASE_TS = datetime(2025, 1, 1, tzinfo=timezone.utc)
RANDOM_SEED = 42

STRUCTURING_SRC = 101
STRUCTURING_TARGETS = list(range(102, 110))  # 102..109
RAPID_PATH = [150, 151, 152, 153]  # 150 → 151 → 152 → 153
TRAVEL_SRC = 160
TRAVEL_TARGETS = [161, 162, 163]
TRAVEL_CURRENCIES = ["USD", "EUR", "GBP"]


# ---------------- helpers ----------------
def _chk(*ids: int, where: str = "") -> None:
    for i in ids:
        if not 1 <= i <= ACCT_ID_MAX:
            raise ValueError(
                f"{where or 'seed'}: 引用不存在的账户 id={i}，"
                f"有效范围 1..{ACCT_ID_MAX}"
            )


def _insert_tx(cur, rows: list[tuple]) -> list[int]:
    """批量插入交易，返回它们的精确 ID（顺序与 rows 一致）。"""
    ids: list[int] = []
    for row in rows:
        cur.execute(
            """INSERT INTO transactions
               (sender_account_id, receiver_account_id, amount, currency, ts, tx_type, status)
               VALUES (%s, %s, %s, %s, %s, %s, %s)
               RETURNING id""",
            row,
        )
        ids.append(cur.fetchone()[0])
    return ids


# ---------------- 主流程 ----------------
def main() -> int:
    random.seed(RANDOM_SEED)

    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DATABASE") or os.getenv("POSTGRES_DB", "antifraud"),
        user=os.getenv("POSTGRES_USER", "antifraud"),
        password=os.getenv("POSTGRES_PASSWORD", "antifraud"),
    )
    cur = conn.cursor()

    # ---------- 1) 清空 ----------
    for t in (
        "fraud_assessments",
        "clearing_records",
        "clearing_batches",
        "transactions",
        "accounts",
        "banks",
    ):
        cur.execute(f"TRUNCATE TABLE {t} RESTART IDENTITY CASCADE")
    conn.commit()
    print("[1/7] 已清空所有表")

    # ---------- 2) banks ----------
    bank_names = [
        "Alpha Bank",
        "Beta Clearing",
        "Gamma Trust",
        "Delta Union",
        "Epsilon Bank",
    ]
    cur.executemany(
        "INSERT INTO banks (name, clearing_code, bic) VALUES (%s, %s, %s)",
        [(n, f"CC{i:03d}", f"BANK{i:02d}XX") for i, n in enumerate(bank_names)],
    )
    conn.commit()
    print(f"[2/7] 已插入 {N_BANKS} 家银行")

    # ---------- 3) accounts ----------
    account_rows = []
    for i in range(N_ACCOUNTS):
        bank_id = (i % N_BANKS) + 1
        acct_type = random.choice(["checking", "savings", "settlement"])
        account_rows.append((bank_id, f"ACC{i:06d}", acct_type))
    cur.executemany(
        "INSERT INTO accounts (bank_id, account_no, acct_type) VALUES (%s, %s, %s)",
        account_rows,
    )
    conn.commit()
    cur.execute("SELECT count(*), min(id), max(id) FROM accounts")
    cnt, lo, hi = cur.fetchone()
    print(f"[3/7] 已插入 {cnt} 个账户 (id {lo}..{hi})")

    # ---------- 4) 正常背景交易 ----------
    normal_rows = []
    for _ in range(N_NORMAL_TX):
        s, r = random.sample(range(1, N_ACCOUNTS + 1), 2)
        ts = BASE_TS + timedelta(
            days=random.randint(0, 364), seconds=random.randint(0, 86399)
        )
        normal_rows.append(
            (
                s,
                r,
                round(random.uniform(10, 5000), 2),
                "USD",
                ts,
                "transfer",
                "completed",
            )
        )
    cur.executemany(
        """INSERT INTO transactions
           (sender_account_id, receiver_account_id, amount, currency, ts, tx_type, status)
           VALUES (%s, %s, %s, %s, %s, %s, %s)""",
        normal_rows,
    )
    conn.commit()
    print(f"[4/7] 已插入 {len(normal_rows)} 笔正常交易")

    # ---------- 5) 三类可疑模式（用 RETURNING id 精确捕获） ----------
    # A: structuring —— 101 在 2h 内向 8 个账户分散转出
    _chk(STRUCTURING_SRC, *STRUCTURING_TARGETS, where="structuring")
    structuring_ts = BASE_TS + timedelta(days=180, hours=10)
    structuring_rows = [
        (
            STRUCTURING_SRC,
            tgt,
            9500.00,
            "USD",
            structuring_ts + timedelta(minutes=10 * k),
            "transfer",
            "completed",
        )
        for k, tgt in enumerate(STRUCTURING_TARGETS)
    ]
    structuring_ids = _insert_tx(cur, structuring_rows)

    # B: rapid_in_out —— 150 → 151 → 152 → 153，12 分钟内 3 跳
    _chk(*RAPID_PATH, where="rapid_in_out")
    rapid_ts = BASE_TS + timedelta(days=200, hours=14)
    rapid_rows = [
        (150, 151, 12000.0, "USD", rapid_ts, "transfer", "completed"),
        (
            151,
            152,
            11800.0,
            "USD",
            rapid_ts + timedelta(minutes=5),
            "transfer",
            "completed",
        ),
        (
            152,
            153,
            11500.0,
            "USD",
            rapid_ts + timedelta(minutes=12),
            "transfer",
            "completed",
        ),
    ]
    rapid_ids = _insert_tx(cur, rapid_rows)

    # C: impossible_travel —— 160 在 6 分钟内以三种币种分别转出
    _chk(TRAVEL_SRC, *TRAVEL_TARGETS, where="impossible_travel")
    travel_ts = BASE_TS + timedelta(days=220, hours=9)
    travel_rows = [
        (
            TRAVEL_SRC,
            TRAVEL_TARGETS[k],
            8000.0 - 100 * k,
            TRAVEL_CURRENCIES[k],
            travel_ts + timedelta(minutes=3 * k),
            "payment",
            "completed",
        )
        for k in range(len(TRAVEL_TARGETS))
    ]
    travel_ids = _insert_tx(cur, travel_rows)

    conn.commit()
    print(
        f"[5/7] 已插入可疑簇："
        f"structuring={len(structuring_ids)} "
        f"rapid_in_out={len(rapid_ids)} "
        f"impossible_travel={len(travel_ids)}"
    )

    # ---------- 6) fraud_assessments（只针对上面精确拿到的 ID） ----------
    def _assess(ids: list[int], risk_type: str) -> list[tuple]:
        return [
            (
                tid,
                risk_type,
                round(random.uniform(75, 99), 2),
                BASE_TS + timedelta(days=365),
            )
            for tid in ids
        ]

    fraud_rows = (
        _assess(structuring_ids, "structuring")
        + _assess(rapid_ids, "rapid_in_out")
        + _assess(travel_ids, "impossible_travel")
    )
    cur.executemany(
        """INSERT INTO fraud_assessments
           (transaction_id, risk_type, risk_score, assessed_at)
           VALUES (%s, %s, %s, %s)""",
        fraud_rows,
    )
    conn.commit()
    print(f"[6/7] 已插入 {len(fraud_rows)} 条欺诈评分")

    # ---------- 7) clearing_batches / clearing_records ----------
    cur.execute("""SELECT id, sender_account_id, receiver_account_id, amount, ts
           FROM transactions ORDER BY id""")
    txs = cur.fetchall()
    cur.execute("SELECT id, bank_id FROM accounts")
    acct_bank: dict[int, int] = dict(cur.fetchall())

    batch_map: dict[tuple[int, object], int] = {}
    batch_totals: dict[tuple[int, object], float] = {}
    record_rows: list[tuple[int, int, str]] = []

    for tid, sender, receiver, amount, ts in txs:
        d = ts.date()
        for bank_id, direction in (
            (acct_bank[sender], "OUT"),
            (acct_bank[receiver], "IN"),
        ):
            key = (bank_id, d)
            if key not in batch_map:
                cur.execute(
                    """INSERT INTO clearing_batches (bank_id, batch_date, total_amount)
                       VALUES (%s, %s, 0) RETURNING id""",
                    (bank_id, d),
                )
                batch_map[key] = cur.fetchone()[0]
                batch_totals[key] = 0.0
            batch_totals[key] += float(amount)
            record_rows.append((batch_map[key], tid, direction))

    cur.executemany(
        "UPDATE clearing_batches SET total_amount = %s WHERE id = %s",
        [
            (round(v, 2), bid)
            for key, bid in batch_map.items()
            for v in [batch_totals[key]]
        ],
    )
    cur.executemany(
        """INSERT INTO clearing_records (batch_id, transaction_id, direction)
           VALUES (%s, %s, %s)""",
        record_rows,
    )
    conn.commit()
    print(f"[7/7] 已插入 {len(batch_map)} 个清算批次、{len(record_rows)} 条清算明细")

    # ---------- 最终复核 ----------
    cur.execute("SELECT count(*) FROM transactions")
    tx_count = cur.fetchone()[0]
    cur.execute("SELECT count(*) FROM fraud_assessments")
    fa_count = cur.fetchone()[0]
    cur.execute("SELECT count(*) FROM clearing_batches")
    cb_count = cur.fetchone()[0]
    cur.execute("SELECT count(*) FROM clearing_records")
    cr_count = cur.fetchone()[0]

    print("=" * 56)
    print(f"banks               = {N_BANKS}")
    print(f"accounts            = {N_ACCOUNTS}")
    print(f"transactions        = {tx_count}")
    print(f"fraud_assessments   = {fa_count}")
    print(f"clearing_batches    = {cb_count}")
    print(f"clearing_records    = {cr_count}")
    print("=" * 56)

    expected_fa = len(structuring_ids) + len(rapid_ids) + len(travel_ids)
    assert tx_count >= 2000, f"transactions 应 ≥ 2000，实际 {tx_count}"
    assert (
        fa_count == expected_fa
    ), f"fraud_assessments 应 == {expected_fa}，实际 {fa_count}"

    cur.close()
    conn.close()
    print("Seed OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
