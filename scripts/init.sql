DROP DATABASE IF EXISTS antifraud WITH (FORCE);
CREATE DATABASE antifraud;
\connect antifraud

-- init.sql
CREATE TABLE banks (
    id            SERIAL PRIMARY KEY,
    name          VARCHAR(128) NOT NULL,
    clearing_code VARCHAR(32)  NOT NULL UNIQUE,
    bic           VARCHAR(16)  NOT NULL
);

CREATE TABLE accounts (
    id        SERIAL PRIMARY KEY,
    bank_id   INTEGER NOT NULL REFERENCES banks(id),
    account_no VARCHAR(32) NOT NULL,
    acct_type  VARCHAR(16) NOT NULL,   -- checking / savings / settlement
    UNIQUE (bank_id, account_no)
);

CREATE TABLE transactions (
    id                BIGSERIAL PRIMARY KEY,
    sender_account_id   INTEGER NOT NULL REFERENCES accounts(id),
    receiver_account_id INTEGER NOT NULL REFERENCES accounts(id),
    amount            NUMERIC(18,2) NOT NULL,
    currency          CHAR(3) NOT NULL DEFAULT 'USD',
    ts                TIMESTAMPTZ NOT NULL,
    tx_type           VARCHAR(24) NOT NULL,  -- transfer / payment / settlement
    status            VARCHAR(16) NOT NULL DEFAULT 'completed'
);

CREATE TABLE clearing_batches (
    id          BIGSERIAL PRIMARY KEY,
    bank_id     INTEGER NOT NULL REFERENCES banks(id),
    batch_date  DATE NOT NULL,
    total_amount NUMERIC(18,2) NOT NULL
);

CREATE TABLE clearing_records (
    id             BIGSERIAL PRIMARY KEY,
    batch_id       BIGINT NOT NULL REFERENCES clearing_batches(id),
    transaction_id BIGINT NOT NULL REFERENCES transactions(id),
    direction      VARCHAR(8) NOT NULL   -- IN / OUT
);

CREATE TABLE fraud_assessments (
    id             BIGSERIAL PRIMARY KEY,
    transaction_id BIGINT NOT NULL REFERENCES transactions(id),
    risk_type      VARCHAR(32) NOT NULL,
    risk_score     NUMERIC(5,2) NOT NULL,
    assessed_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
