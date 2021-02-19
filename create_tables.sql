CREATE TABLE stock (
 id SERIAL PRIMARY KEY,
 symbol TEXT NOT NULL,
 name TEXT NOT NULL,
 country TEXT,
 sector TEXT,
 industry TEXT,
 exchange TEXT
);

CREATE TABLE mention (
    stock_id INTEGER,
    dt TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    message TEXT NOT NULL,
    source TEXT NOT NULL,
    url TEXT NOT NULL,
    PRIMARY KEY (stock_id, dt),
    CONSTRAINT fk_mention_stock FOREIGN KEY (stock_id) REFERENCES stock (id)
);

CREATE INDEX ON mention (stock_id, dt DESC);
SELECT create_hypertable('mention', 'dt');
