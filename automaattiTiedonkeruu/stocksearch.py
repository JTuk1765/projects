import os
import datetime
import requests
import sys
import time
import yfinance as yf
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from supabase import create_client, Client
from datetime import date
import math
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import os
from dotenv import load_dotenv
import re

# Loads variables to .env
load_dotenv()


def connection(error_path):
    ''' Makes sure that connection to internet is available and if not, writes the
    error code down and quits program
    
    error_path: file with error codes
    '''
    # empties the file from last runs
    with open(error_path, 'w', encoding='utf-8') as ef:
        pass

    # checks the internet connection
    test_url = "https://1.1.1.1"
    last_exception = None
    entries = 5
    wait = 20

    for attemp in range(1, entries + 1):
        try: 
            response = requests.get(test_url, timeout = 5)
            return True
        except requests.RequestException as e:
            last_exception = e

            if attemp < entries:
                time.sleep(wait)

    with open(error_path, 'a', encoding='utf-8') as ef:
        ef.write(f"internetyhteyttä ei saatu {entries} yrityksen jälkeen. "
                 f"viimeinen koodi: {last_exception}")

    sys.exit(1)
    

def stock_list(path, error_path, target_industries, no_go_industries):
    ''' This function connects to Nasdaq API and gets every stock in USA. It
    sorts companies with wanted industries and saves stocks to a file.

    path: file where stocks are being saved
    error_path: file where possible error code is saved
    target_industries: list of wanted industries
    no_go_industries: unwanted industries list

    '''

    # log in info
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json, text/plain, */*'
    }

    # source
    url = 'https://api.nasdaq.com/api/screener/stocks?download=true'

    try:
        response = requests.get(url, headers = headers, timeout=15)

        # if connection is successfull
        # saves every stock in USA
        if response.status_code == 200:
            data = response.json().get('data', {}) 
            rows = data.get('rows') or data.get('table', {}).get('rows', [])
            filtered_symbols = []

            # sorts out empty rows and saves
            for row in rows:
                symbol = row.get('symbol', '').replace('.', '-')
                industry = row.get('industry', '') or ''
                sector = row.get('sector', '') or ''

                if '^' in symbol or '/' in symbol or not symbol:
                    continue

                # makes sure that sector and industry are written on lower 
                ind_lower = industry.lower()
                sec_lower = sector.lower()

                # if industry or sector is in no go list, skip that
                if any(kw.lower() in ind_lower or kw.lower() in sec_lower for kw in 
                    no_go_industries):
                    continue
                # if industry or sector is correct saves that stock
                if any(kw.lower() in ind_lower or kw.lower() in sec_lower for kw in 
                    target_industries):
                    filtered_symbols.append(symbol)
                    

            # writing symbols to path file
            with open(path, 'w', encoding='utf-8') as f:
                for smbl in sorted(filtered_symbols):
                    f.write(f"{smbl}\n")

    # if any errors during the process
        else: 
            with open(error_path, 'a', encoding='utf-8') as ef:
                ef.write (f"Haku epäonnistui statuskoodilla: {response.status_code}\n")

    except Exception as e:
        with open(error_path, 'a', encoding='utf-8') as ef:
            ef.write(f"Kriittinen virhe stock_list-funktiossa: {e}\n")


def list_of_stocks(file, error_file):
    ''' Opens file where the symbols are and saves it to a list, if list does not open,
    saves the reason into error file.

    file: file where symbols are listed
    error_file: file for errors

    return: stock_from_file: list of symbols
    '''

    # checks incase file is empty
    with open(file, 'r', encoding='utf-8') as f:
        if sum(1 for line in f if line.strip()) <= 0:
            with open(error_file, 'a', encoding='utf-8') as ef:
                ef.write(f"Symbol tiedosto on tyhjä\n")
                sys.exit(1)

    # reads stocks and adds symbols to list
    stocks_from_file = []
    try:
        with open(file, "r") as file:
            for line in file:
                stocks_from_file.append(line.strip().upper())

    # incase of errors
    except Exception as e: 
        with open(error_file, "a", encoding="utf-8") as ef:
            ef.write(f"Osake listaa muodostaessa tiedoston avauksessa {e}\n")
            sys.exit(1)

    return stocks_from_file


def gather_data(symbol, criteria, error_file):
    """ Gathers wanted data from yahoo finance.

    symbol: spesific stock
    criteria: list of wanted infos
    error_file: file for possible errors

    return: all data from certain stock. 
    """

    # Function from "empiirinen projekti" data gather.
    try: 
        stock = yf.Ticker(symbol)
        info = stock.info

        data = {"Ticker": symbol}
        for limit in criteria:
            data[limit] = info.get(limit)

        # Searches history
        history = stock.history(period="1y")
        if not history.empty:
            end_price = history['Close']
            # counts yearly_return
            data['yearlyReturn'] = (end_price.iloc[-1] / end_price.iloc[0]) - 1
            # counts Yearly Volatility
            data['yearlyVolatility'] = end_price.pct_change().std() * (252 ** 0.5)

        return data
    except Exception as e:
        with open(error_file, "a", encoding="utf-8") as ef:
            ef.write(f"Virhe osakkeiden hakemisessa gather_data funktiossa: {e}\n")
            return None


def workers(stocks, infos, error_file):
    """ Defines how many workers are being used, length of pauses and chunk 
    sizes. It calls gather_data function returns all data. 

    stocks: list of stocks to search
    error_file: place to save error codes

    return: raw data of data scraping
    """

    results = []   # list for results of data gather
    num_workers = 5   # number of API askings at the same time
    chunk_size = 300   # how many stocks at once
    pause_duration = 180   # 3 minutes if more than chunk_size of stocks

    for i in range(0, len(stocks), chunk_size):
        chunk = stocks[i: i + chunk_size]

        # Creates a thread pool for concurrent data retrieval and executes 
        # gather_data for multiple stocks simultaneously to minimize network 
        # wait times. Future objects stored in the dictionary enable handling results as they complete.
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            future_to_stocks = {executor.submit(gather_data, s, infos, error_file): s for s in chunk}

            for future in future_to_stocks:
                res = future.result()
                if res:
                    results.append(res)

        # if list still contains stocks
        if i + chunk_size < len(stocks):
            time.sleep(pause_duration)

    return results


def is_renewable_company(summary, wanted_list, unwanted_list):
    """
    Checks if there is keywords in longbusinesssummary

    summary: business summary
    wanted_list: wanted keywords
    unwanted_list: unwanted keywords

    return: amount of keywords found
    """
    if not isinstance(summary, str) or not summary:
        return False
    
    # builds patterns
    # Done with AI
    wanted_pattern = r'\b(' + '|'.join(wanted_list) + r')\b'
    unwanted_pattern = r'\b(' + '|'.join(unwanted_list) + r')\b'

    # counts keywords from patterns
    wanted_count = len(re.findall(wanted_pattern, summary, re.IGNORECASE))
    unwanted_count = len(re.findall(unwanted_pattern, summary, re.IGNORECASE))
    
    # conditions and return
    return wanted_count > 0 and wanted_count >= unwanted_count


def sorter(data, error_file):
    ''' sorts out wrong industries and groups by valuation into two groups:
    'undervaluated' and 'others'. 

    data: all data to be sorted
    error_file: file for errors

    return: two groups of data
    '''
    if not data:
        e = pd.DataFrame()
        with open(error_file, "a", encoding="utf-8") as ef:
            ef.write(f"virhe sorter funktiossa: {e}")
            return pd.DataFrame()

    df = pd.DataFrame(data)

    # stock exchange limits
    df['fullExchangeName'] = df['fullExchangeName'].astype(str).str.upper()
    condition = df['fullExchangeName'].str.contains('NASDAQ|NYSE|NMS|NYQ')

    # industry sorter
    wanted = ['solar', 'wind', 'hydroelectric', 'geothermal', 'hydrogen', 'fuel cell', 
              'energy storage', 'renewable', 'clean energy']
    unwanted = ['coal', 'natural gas', 'petroleum', 'crude oil', 'pipeline', 'refining']

    if 'longBusinessSummary' in df.columns:
        df = df[df['longBusinessSummary'].apply(
            lambda x: is_renewable_company(x, wanted, unwanted)
        )]
     

    if df.empty:
        return pd.DataFrame()

    # checks if the market value is correct and sorts out bad companies
    # market value sorter
    condition1 = df["marketCap"] < 5_000_000_000
    all_df = df[condition1].sort_values(by="marketCap", ascending=True)

    # checks that wanted criteria ar in numeral format
    cols = ['marketCap', 'trailingPE', 'priceToBook', 'beta']
    for col in cols:
        if col in all_df.columns:
            all_df[col] = pd.to_numeric(all_df[col], errors='coerce')
        else:
            all_df[col] = None

    # deletes negative p/e for mean count
    valid_pe = all_df[all_df['trailingPE'] > 0]['trailingPE']
    valid_pb = all_df[all_df['priceToBook'] > 0]['priceToBook']
    
    mean_pe = valid_pe.mean()
    mean_pb = valid_pb.mean()
    mean_beta = all_df['beta'].mean()

    # Conditions
    c1 = (all_df['trailingPE'] > 0) & (all_df['trailingPE'] < mean_pe)
    c2 = (all_df['priceToBook'] > 0) & (all_df['priceToBook'] < mean_pb)
    c3 = (all_df['beta'] < mean_beta)

    # Counts ifs that happened
    match_count = c1.astype(int) + c2.astype(int) + c3.astype(int)

    # At least 2 of 3 needs to happen
    final_condition = (all_df['trailingPE'] > 0) & (match_count >= 2)

    # filtered stocks
    filtered_df = all_df[final_condition].sort_values(by='trailingPE', ascending=True)
    filtered_df = filtered_df.drop(columns=['longBusinessSummary'], errors='ignore')
    filtered_df = filtered_df.drop(columns=['fullExchangeName'], errors='ignore')

    all_df = all_df.drop(columns=['longBusinessSummary'], errors='ignore')
    all_df = all_df.drop(columns=['fullExchangeName'], errors='ignore')

    return all_df, filtered_df


def sql_database(all_data, filtered_data, error_file):
    '''
    Saves all data to sql database. Makes sure that allready excisting stocks are
    being updated and stocks that are no longer undervalued are being deleted. This
    function is mostly done with AI and copy/paste and slight changes of my own.

    all_data: df with all the data of every stock with market value under wanted
    filtered_data: df with only undervalued stocks
    error_file: error file

    '''
    # keys
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")

    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    today = date.today().isoformat()
    all = []
    filtered = []

    # changes all_data Dataframe to dictionary list
    all1 = all_data.to_dict(orient="records")

    for item in all1:
        row = {}

        # adds date
        row["date_id"] = today

        # other fields
        for key, value in item.items():
            # Changes to small letters
            clean_key = key.lower()

            # "ticker" -> "symbol" 
            if clean_key == "ticker":
                clean_key = "symbol"

            # NaN values to NULL (SQL NULL)
            if isinstance(value, float) and math.isnan(value):
                value = None

            row[clean_key] = value

        all.append(row)

        data_inserted_succesfully = False

    # changes filtered_data Dataframe to dictionary list
    filtered1 = filtered_data.to_dict(orient="records")
        
    for item in filtered1:
        row = {}
        
        # adds date
        row["date_id"] = today
        
        # other fields
        for key, value in item.items():
            # Changes to small letters
            clean_key = key.lower()
        
            # "ticker" -> "symbol" 
            if clean_key == "ticker":
                clean_key = "symbol"
        
            # NaN values to NULL (SQL NULL)
            if isinstance(value, float) and math.isnan(value):
                value = None
        
            row[clean_key] = value
        
        filtered.append(row)

    # Saving to supabase
    try:
        batch_size = 100
        for i in range(0, len(all), batch_size):
            chunk = all[i : i + batch_size]
            # upsert updates the row if same date and symbol
            supabase.table("stock_data").upsert(chunk, on_conflict="date_id, symbol").execute()
        data_inserted_succesfully = True

    except Exception as e:
        with open(error_file, "a", encoding="utf-8") as ef:
            ef.write(f"Virhe Supabase-tallennuksessa (data-taulu): {e}\n")


    # mean 
    try:
        # group by industry
        mean_df = (
            all_data.groupby("industry", as_index=False)
            .agg({
                "trailingPE": "mean",
                "priceToBook": "mean",
                "beta": "mean",
                "enterpriseToEbitda": "mean"
            })
            .rename(columns={
                "trailingPE": "mean_pe",
                "priceToBook": "mean_pb",
                "beta": "mean_beta",
                "enterpriseToEbitda": "mean_ev_ebitda"
            })
        )
        mean_df["date_id"] = today

        mean_dicts = mean_df.to_dict(orient="records")

        # NaN -> None (SQL NULL)
        for row in mean_dicts:
            for k, v in row.items():
                if isinstance(v, float) and math.isnan(v):
                    row[k] = None

        supabase.table("mean").upsert(mean_dicts, on_conflict="date_id, industry").execute()
    except Exception as e:
        with open(error_file, "a", encoding="utf-8") as ef:
            ef.write(f"Virhe Supabase-tallennuksessa (mean-taulu): {e}\n")

        if not data_inserted_succesfully or len(all) == 0:
            with open(error_file, "a", encoding="utf-8") as ef:
                ef.write("Ajo keskeytetty positioiden osalta: Dataa ei saatu kantaan.\n")
            return

    # positions
    try: 
        # creates dict with price and symbol
        current_run = {}
        for item in filtered:
            sym = item.get("symbol")
            price = item.get("currentprice")
            if sym and price is not None:
                current_run[sym] = price

        # searches for open positions from supabase
        response = supabase.table("positions").select("*").eq("status", "OPEN").execute()
        open_positions = response.data if response.data else []

        open_symbols = set()

        # goes through open positions that exist
        for pos in open_positions:
            pos_id = pos["id"]
            sym = pos["symbol"]
            open_symbols.add(sym)

            entry_date = datetime.date.fromisoformat(pos["entry_date"])
            entry_price = float(pos["entry_price"]) if pos.get("entry_price") else 0.0
            today_date = datetime.date.fromisoformat(today)
            days_held = (today_date - entry_date).days

            # checks the terms  
            # A: symbol not found in this run
            # B: held for 365+ days

            is_still_underval = sym in current_run
            if not is_still_underval or days_held >= 365:
                exit_price = current_run.get(sym, pos.get("last_price", entry_price))
                exit_price = float(exit_price) if exit_price else entry_price

                return_pct = ((exit_price - entry_price) / entry_price * 100) if entry_price > 0 else 0.0
                close_reason = "MAX_TIME" if days_held >= 365 else "FAIR_VALUE_REACHED"

                # closes position in supabase
                supabase.table("positions").update({
                "status": "CLOSED",
                "exit_date": today,
                "exit_price": exit_price,
                "return_pct": round(return_pct, 2),
                "days_held": days_held,
                "close_reason": close_reason
                }).eq("id", pos_id).execute()

            else:
                latest_price = current_run[sym]
                supabase.table("positions").update({
                    "last_price": latest_price,
                    "days_held": days_held
                }).eq("id", pos_id).execute()

        # opens new positions for new stocks
        for sym, price in current_run.items():
            if sym not in open_symbols:
                new_pos = {
                    "symbol": sym,
                    "entry_date": today,
                    "entry_price": price,
                    "last_price": price,
                    "days_held": 0,
                    "status": "OPEN"
                }
                supabase.table("positions").insert(new_pos).execute()

    except Exception as e:
        with open(error_file, "a", encoding="utf-8") as ef:
            ef.write(f"Virhe Supabase-tallennuksessa (positions-taulu): {e}\n")



def send_email_summary(summary_content, sender_email, app_password, recipient_email, error_file):
    """ 
    Sends email summary to user with summary file. Help functon. This funvtion 
    is mostly done with AI

    summary_content: content to be sent
    sender_email: address
    app_password: passkey
    recipient_email: adress
    error_file: errors

    """

    try:
        # Creates message
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = recipient_email
        msg['Subject'] = f"StockSearch - Ajon yhteenveto {date.today().isoformat()}"

        # Adds txt file
        msg.attach(MIMEText("Liitteenä päivän yhteenveto osake hausta.", "plain", "utf-8"))

        if os.path.exists(summary_content):
            with open(summary_content, "rb") as f:
                filename = os.path.basename(summary_content)
                part = MIMEApplication(f.read(), Name=filename)
                part['Content-Disposition'] = f'attachment; filename="{filename}"'
                msg.attach(part)

        if os.path.exists(error_file):
            with open(error_file, "rb") as f:
                filename = os.path.basename(error_file)
                fail = MIMEApplication(f.read(), Name=filename)
                fail['Content-Disposition'] = f'attachment; filename="{filename}"'
                msg.attach(fail)
            
            

        # Creates connection to Gmail SMTP-server
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()  # protected TLS-connection
            server.login(sender_email, app_password)
            server.send_message(msg)

    except Exception as e:
        with open(error_file, "a", encoding="utf-8") as ef:
            ef.write(f"Virhe sähköpostin lähetyksessä: {e}\n")


def save_summary(today, start_time, total_stocks, filtered_df, error_file, summary_file="stock.txt"):
    """
    Creates compilation of data and stocks to be send. Uses send_email_summary 
    function to send.

    today: date of today
    start_time: when project begin
    total_stocks: amount of stocks that are being sorted
    filtered_df: list of undervalued stocks
    error_file: file for errors
    summary_file: where is info saved

    """
    end_time = time.time()
    duration = round(end_time - start_time, 2)
    start_str = datetime.datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')
    end_str = datetime.datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S')

    error_count = 0
    if os.path.exists(error_file):
        with open(error_file, 'r', encoding='utf-8') as ef:
            error_count = sum(1 for line in ef if line.strip())

    matching_count = len(filtered_df)

    mean_pe = filtered_df['trailingPE'].mean() if not filtered_df.empty and 'trailingPE' in filtered_df else None
    mean_pb = filtered_df['priceToBook'].mean() if not filtered_df.empty and 'priceToBook' in filtered_df else None
    mean_beta = filtered_df['beta'].mean() if not filtered_df.empty and 'beta' in filtered_df else None
    mean_market_cap = filtered_df['marketCap'].mean() if not filtered_df.empty and 'marketCap' in filtered_df else None

    with open(summary_file, "w", encoding="utf-8") as f:
        f.write(f"--AJON YHTEENVETO--\n")
        f.write(f"päivämäärä: {today}\n")
        f.write(f"kellonaika: {datetime.datetime.now().strftime('%H:%M:%S')}\n")
        f.write(f"\n")
        f.write(f"\n")

        f.write(f"HAUN TIEDOT:\n")
        f.write(f"Aloitusaika: {start_str}\n")
        f.write(f"Lopetusaika: {end_str}\n")
        f.write(f"Kesto sekunteina:  {duration} s\n")
        f.write(f"\n")
        f.write(f"Löydettyjä osakkeita: {total_stocks} kpl \n")
        f.write(f"Sopivat osakkeet:  {matching_count} kpl\n")
        f.write(f"Virheiden määrä:   {error_count} kpl\n")
        f.write(f"\n")

        f.write("--- KESKIARVOT (Sopivat osakkeet) ---\n")
        f.write(f"Markkina-arvo ka:  {mean_market_cap:,.0f} $\n")

        f.write(f"P/E keskiarvo:     {round(mean_pe, 2)}\n")
        f.write(f"P/B keskiarvo:     {round(mean_pb, 2)}\n")
        f.write(f"Beta keskiarvo:    {round(mean_beta, 2)}\n\n")

        f.write("--- SOPIVAT OSAKKEET ---\n")
        if not filtered_df.empty:
            for _, row in filtered_df.iterrows():
                # 
                name = row.get('longName') or 'Tuntematon'
                symbol = row.get('shortName') or row.get('Ticker') or 'Tuntematon'
                industry = row.get('industry') or 'Tuntematon'
                price = row.get('currentPrice') or row.get('previousClose') or row.get('price') or 'N/A'

                f.write(f"Nimi: {name} | SMBL: {symbol} | Toimiala: {industry} | Hinta: {price}\n")
        else:
            f.write("Ei kriteerit täyttäviä osakkeita.\n") 

        f.write(f"\n")
        f.write(f"Osakkeet on tallennettu Supabase tietokantaan.\n")
        f.write(f"JONNI TUKI (automaattinen ohjelma)")

    SENDER_EMAIL = "jonni.tuki@gmail.com"
    APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD")   # Google password
    RECIPIENT_EMAIL = "jonni.tuki@gmail.com"

    send_email_summary(
        summary_content=summary_file,
        sender_email=SENDER_EMAIL,
        app_password=APP_PASSWORD,
        recipient_email=RECIPIENT_EMAIL,
        error_file=error_file)


def data_scraping(file, error_file, criteria, today, start_time):
    ''' This function is the base of data gather, it calls other fuctions and together they
    collect all the informations from symbol list
    '''

    # makes a list out of symbols
    symbols = list_of_stocks(file, error_file)
    total_symbols = len(symbols)

    # defines info about workers and runs the data collecting through another function
    # saves all the collected data to all_data
    all_data = workers(symbols, criteria, error_file)

    # deletes if criteria doesnt match
    all_df, filtered_df = sorter(all_data, error_file)

    # saves all data to sql database
    sql_database(all_df, filtered_df, error_file)

    # saving data for user
    save_summary(today, start_time, total_symbols, filtered_df, error_file, summary_file="stock.txt")


def main():
    # program defines the following things
    start_time = time.time()
    today = datetime.date.today().isoformat() 

    # path of stock symbol list
    # enter the paths of the file
    path = "symbol_list.txt"   
    error_path = "errors.txt"

    connection(error_path)

    # criteria for the data collecting as a list (can be altered)
    criteria = ['sector', 'industry', 'longName', 'currentPrice', 'marketCap', 
                'trailingPE', 'forwardPE', 'priceToBook', 'priceToSalesTrailing12Months', 
                'beta', 'returnOnEquity', 'revenueGrowth', 'enterpriseToEbitda', 
                'dividendYield', 'freeCashflow', 'earningsGrowth', 'operatingMargins',
                'quickRatio', 'currentRatio', 'shortPercentOfFloat', 'targetMeanPrice', 
                'longBusinessSummary', 'fullExchangeName']  

    # list of wanted industries
    target_industries = ['renewable', 'solar', 'uranium', 'electric', 'power', 
                         'alternative energy', 'utilities', 'semiconductor',
                         'electrical equipment', 'renewable', 'clean tech',
                         'hydrogen', 'battery', 'energy storage']
    # list of unwanted industries
    no_go_industries = ['oil', 'gas', 'petroleum', 'fossil', 'coal', 'exploration', 
                        'drilling', 'pipeline', 'refining']

    # seaches for newest list of stocks 
    stock_list(path, error_path, target_industries, no_go_industries)

    # data gathering begins
    data_scraping(path, error_path, criteria, today, start_time)


if __name__ == "__main__":
    main()