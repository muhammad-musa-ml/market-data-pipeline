-- Applied to each of api_prod and api_test by serving-init. These tables are the MySQL backend
-- of the REST API and have nothing to do with stage 1's symbols and bars in database market.

CREATE TABLE IF NOT EXISTS companies (
  ticker VARCHAR(8) NOT NULL PRIMARY KEY,
  name VARCHAR(80) NOT NULL,
  sector VARCHAR(32) NOT NULL,
  exchange VARCHAR(16) NOT NULL,
  KEY ix_companies_sector (sector)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS daily_bars (
  ticker VARCHAR(8) NOT NULL,
  trade_date DATE NOT NULL,
  `open` DECIMAL(12,4) NOT NULL,
  high DECIMAL(12,4) NOT NULL,
  low DECIMAL(12,4) NOT NULL,
  `close` DECIMAL(12,4) NOT NULL,
  volume BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (ticker, trade_date),
  CONSTRAINT fk_daily_bars_company FOREIGN KEY (ticker) REFERENCES companies (ticker)
) ENGINE=InnoDB
