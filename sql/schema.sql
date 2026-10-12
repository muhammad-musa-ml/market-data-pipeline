CREATE TABLE IF NOT EXISTS symbols (
  symbol_id SMALLINT UNSIGNED NOT NULL PRIMARY KEY,
  ticker VARCHAR(8) NOT NULL,
  name VARCHAR(80) NOT NULL,
  sector VARCHAR(32) NOT NULL,
  exchange VARCHAR(16) NOT NULL,
  UNIQUE KEY uq_symbols_ticker (ticker),
  KEY ix_symbols_sector (sector),
  KEY ix_symbols_exchange (exchange)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS bars (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  symbol_id SMALLINT UNSIGNED NOT NULL,
  trade_date DATE NOT NULL,
  `open` DECIMAL(12,4) NOT NULL,
  high DECIMAL(12,4) NOT NULL,
  low DECIMAL(12,4) NOT NULL,
  `close` DECIMAL(12,4) NOT NULL,
  volume BIGINT UNSIGNED NOT NULL,
  inserted_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  UNIQUE KEY uq_bars_symbol_date (symbol_id, trade_date),
  KEY ix_bars_date (trade_date),
  CONSTRAINT fk_bars_symbol FOREIGN KEY (symbol_id) REFERENCES symbols (symbol_id)
) ENGINE=InnoDB
