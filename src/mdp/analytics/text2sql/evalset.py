"""Questions with hand-written gold SQL, for scoring text-to-SQL by execution accuracy.

None of them names a generated ticker, so they hold for any seed. Gold SQL has an ORDER BY only
where the question asks for an order or a top-k, and the scorer compares in order only then.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class Case:
    id: str
    question: str
    gold: str


CASES = [
    Case("count-symbols", "How many symbols are there?",
         "SELECT COUNT(*) FROM market.symbols"),
    Case("nasdaq-symbols", "How many symbols are listed on NASDAQ?",
         "SELECT COUNT(*) FROM market.symbols WHERE exchange = 'NASDAQ'"),
    Case("count-sectors", "How many different sectors are there?",
         "SELECT COUNT(DISTINCT sector) FROM market.symbols"),
    Case("count-bars", "How many daily bars are in the warehouse?",
         "SELECT COUNT(*) FROM market.bars"),
    Case("max-close", "What is the highest closing price ever recorded?",
         "SELECT MAX(close) FROM market.bars"),
    Case("date-range", "What are the first and the last trading dates?",
         "SELECT MIN(trade_date), MAX(trade_date) FROM market.bars"),
    Case("energy-symbols", "How many Energy symbols are there?",
         "SELECT COUNT(*) FROM market.symbols WHERE sector = 'Energy'"),
    Case("symbols-per-sector", "How many symbols are in each sector?",
         "SELECT sector, COUNT(*) FROM market.symbols GROUP BY sector"),
    Case("avg-close-exchange", "What is the average closing price on each exchange?",
         "SELECT s.exchange, AVG(b.close) FROM market.bars b "
         "JOIN market.symbols s ON b.symbol_id = s.symbol_id GROUP BY s.exchange"),
    Case("volume-2022", "What was the total volume traded in 2022?",
         "SELECT SUM(volume) FROM market.bars WHERE year(trade_date) = 2022"),
    Case("up-days", "On how many bars was the close above the open?",
         "SELECT COUNT(*) FROM market.bars WHERE close > open"),
    Case("top-volume-tickers", "Which three tickers have the largest total volume? Largest first.",
         "SELECT ticker FROM market.bars GROUP BY ticker ORDER BY SUM(volume) DESC LIMIT 3"),
    Case("avg-volume-sector", "What is the average daily volume in each sector?",
         "SELECT s.sector, AVG(b.volume) FROM market.bars b "
         "JOIN market.symbols s ON b.symbol_id = s.symbol_id GROUP BY s.sector"),
    Case("nyse-days-2021", "On how many distinct dates in 2021 did NYSE-listed symbols have a bar?",
         "SELECT COUNT(DISTINCT b.trade_date) FROM market.bars b "
         "JOIN market.symbols s ON b.symbol_id = s.symbol_id "
         "WHERE s.exchange = 'NYSE' AND year(b.trade_date) = 2021"),
    Case("busiest-day", "Which ticker had the largest single-day volume, and what was that volume?",
         "SELECT ticker, volume FROM market.bars ORDER BY volume DESC LIMIT 1"),
    Case("bars-per-year", "How many bars are there in each year? Order by year.",
         "SELECT year(trade_date) AS y, COUNT(*) FROM market.bars GROUP BY year(trade_date) ORDER BY y"),
    Case("tech-jan-2023", "What was the average closing price of Technology symbols in January 2023?",
         "SELECT AVG(b.close) FROM market.bars b JOIN market.symbols s ON b.symbol_id = s.symbol_id "
         "WHERE s.sector = 'Technology' AND year(b.trade_date) = 2023 AND month(b.trade_date) = 1"),
    Case("big-movers", "How many different tickers ever had a daily return above 0.05?",
         "SELECT COUNT(DISTINCT ticker) FROM market.daily_returns WHERE ret > 0.05"),
    Case("worst-day", "Which ticker and date had the lowest daily return, and what was it?",
         "SELECT ticker, trade_date, ret FROM market.daily_returns WHERE ret IS NOT NULL "
         "ORDER BY ret ASC LIMIT 1"),
    Case("expensive-per-exchange", "For each exchange, how many symbols have an average closing price above 100?",
         "SELECT exchange, COUNT(*) FROM (SELECT s.exchange, s.symbol_id FROM market.bars b "
         "JOIN market.symbols s ON b.symbol_id = s.symbol_id GROUP BY s.exchange, s.symbol_id "
         "HAVING AVG(b.close) > 100) t GROUP BY exchange"),
    Case("range-2020-sector", "What is the average high-low range per sector in 2020?",
         "SELECT s.sector, AVG(b.high - b.low) FROM market.bars b "
         "JOIN market.symbols s ON b.symbol_id = s.symbol_id WHERE year(b.trade_date) = 2020 "
         "GROUP BY s.sector"),
    Case("pct-up", "What percentage of bars closed above their open?",
         "SELECT 100.0 * AVG(CASE WHEN close > open THEN 1.0 ELSE 0.0 END) FROM market.bars"),
    # added after the first 22 all passed: top-per-group, a view joined back to symbols, set logic
    Case("top-ticker-per-sector", "For each sector, which ticker has the highest average closing price?",
         "SELECT sector, ticker FROM (SELECT s.sector, s.ticker, "
         "RANK() OVER (PARTITION BY s.sector ORDER BY AVG(b.close) DESC) AS r "
         "FROM market.bars b JOIN market.symbols s ON b.symbol_id = s.symbol_id "
         "GROUP BY s.sector, s.ticker) t WHERE r = 1"),
    Case("busiest-year", "Which year had the largest total volume, and how large was it?",
         "SELECT year(trade_date), SUM(volume) FROM market.bars GROUP BY year(trade_date) "
         "ORDER BY SUM(volume) DESC LIMIT 1"),
    Case("net-risers", "How many tickers closed higher on their last trading day than on their first?",
         "SELECT COUNT(*) FROM (SELECT ticker FROM market.bars GROUP BY ticker "
         "HAVING max_by(close, trade_date) > min_by(close, trade_date)) t"),
    Case("avg-return-sector-2022", "What was the average daily return of each sector in 2022?",
         "SELECT s.sector, AVG(r.ret) FROM market.daily_returns r "
         "JOIN market.symbols s ON r.ticker = s.ticker WHERE year(r.trade_date) = 2022 GROUP BY s.sector"),
    Case("broad-up-days", "On how many dates did more than half of all tickers close above their open?",
         "SELECT COUNT(*) FROM (SELECT trade_date FROM market.bars GROUP BY trade_date "
         "HAVING AVG(CASE WHEN close > open THEN 1.0 ELSE 0.0 END) > 0.5) t"),
    Case("top-ticker-per-exchange", "For each exchange, which ticker has the largest total volume?",
         "SELECT exchange, ticker FROM (SELECT s.exchange, b.ticker, "
         "RANK() OVER (PARTITION BY s.exchange ORDER BY SUM(b.volume) DESC) AS r "
         "FROM market.bars b JOIN market.symbols s ON b.symbol_id = s.symbol_id "
         "GROUP BY s.exchange, b.ticker) t WHERE r = 1"),
]
