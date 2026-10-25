-- 1. Keskiarvot-taulu
CREATE TABLE IF NOT EXISTS mean (
    date_id DATE NOT NULL, 
    industry VARCHAR (50) NOT NULL,
    mean_pe NUMERIC(10, 2),
    mean_pb NUMERIC(10, 2),
    mean_beta NUMERIC(10, 2),
    mean_ev_ebitda NUMERIC(10, 2), 
    PRIMARY KEY (date_id, industry)
);

-- 2. Data-taulu (Säilöö kaikkien seurattavien yhtiöiden tiedot tiettynä päivänä)
CREATE TABLE IF NOT EXISTS stock_data (
    id SERIAL PRIMARY KEY,
    date_id DATE NOT NULL, 
    symbol VARCHAR(10) NOT NULL,
    longname TEXT,
    sector VARCHAR(50),
    industry VARCHAR(50) NOT NULL,
    marketCap NUMERIC (20, 2),
    currentPrice NUMERIC (10, 3),
    trailingpe NUMERIC (10, 2),
    forwardpe NUMERIC (10, 2),
    priceToBook NUMERIC (10, 2),
    pricetosalestrailing12months NUMERIC (10, 2),
    beta NUMERIC (10, 2),
    returnOnEquity NUMERIC (10, 2),
    revenueGrowth NUMERIC (10, 2),
    YearlyReturn NUMERIC (10, 2),
    YearlyVolatility NUMERIC (10, 2),
    enterpriseToEBITDA NUMERIC (10, 2),
    dividendYield NUMERIC (10, 2),
    freeCashFlow NUMERIC (20, 2),
    earningsGrowth NUMERIC (10, 2),
    operatingMargins NUMERIC (10, 2),
    quickRatio NUMERIC (10, 2),
    currentRatio NUMERIC (10, 2),
    shortPercentOfFloat NUMERIC (10, 2),
    targetMeanPrice NUMERIC (10, 2),
    
    -- Varmistaa, että samalla osakkeella on vain yksi merkintä per päivämäärä
    CONSTRAINT unique_symbol_per_date UNIQUE (date_id, symbol)

);

-- 3. Seuranta_lista-taulu (Seuraa vain valittuja osakkeita ja niiden päivittäisiä hintoja)
CREATE TABLE IF NOT EXISTS positions (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    entry_date DATE NOT NULL,
    entry_price NUMERIC,
    last_price NUMERIC,
    exit_date DATE,
    exit_price NUMERIC,
    return_pct NUMERIC,
    days_held INT DEFAULT 0,
    status VARCHAR(10) DEFAULT 'OPEN',
    close_reason VARCHAR(30)
);