import sqlite3
from datetime import datetime
from typing import Any, Dict
import yfinance as yf
from fastapi import FastAPI
from openai import OpenAI
from pydantic import BaseModel, Field
from sqlalchemy import create_engine

DB_FILE = "/usr/games/stockstats.db"
DB_URL = f"sqlite:///{DB_FILE}"


app = FastAPI(title="Market Analysis Tool")
datenow = datetime.now()
datenow = datenow.strftime("%Y-%m-%d")


class Request(BaseModel):
    query: str


class Response(BaseModel):
    response: Dict[str, Any]


class Tools:
    class Valves(BaseModel):
        OPENAI_API_KEY: str = Field("", description="Your OpenAI API key")

    def __init__(self):
        self.Valves = self.Valves(OPENAI_API_KEY="")

    def redgreenblue(self, query):
        userquery = query
        client = OpenAI(api_key=OPENAI_API_KEY)
        prompt = f"""
        You are a Stock and Time Bot. Your only purpose is to analyze and understand the user's prompt
        and return the exact formatted answer. This formatted answer can be found as:
        The TICKER(s) of a company, all seperated by one space, then exactly after that, with one space, the START date and END date,
        formatted as YYYY-MM-DD. An Example: TSLA START_DATE END_DATE AAPL START_DATE, END_DATE. Do this for all tickers. All entries must be seperated by one space. If either the start or end date are found to be NON-TRADING
        DAYS, (Weekends/Holidays for NYSE), put the date as the first trading day after the weekend or holiday. If no date is specified
        you are to PULL THE LAST 6 MONTHS from 2025-8-18. ALL OF THIS MUST BE FORMATTED AS SPECIFIED ABOVE.
        IF the user asks for a specific query, make the start and end date the same. 
        If you cant find any meaningful data, return QUIT.
        """
        formattedprompt = prompt.format()
        # noinspection PyTypeChecker
        resp = client.responses.create(
            model="gpt-5-mini",
            instructions=formattedprompt,
            reasoning={"effort": "high"},
            input=userquery,
        )
        if resp.output_text == "QUIT":
            raise NotImplementedError()
        return resp.output_text

    def thing2(self, query):
        arrayify = self.redgreenblue(query)
        arrayify = arrayify.split()
        return arrayify

    def sql_query(self, query):
        try:
            tokens = self.thing2(query)
            if len(tokens) % 3 != 0:
                raise ValueError("Expected triples...")

            price_sql = """
                SELECT *
                FROM price_daily
                WHERE "Ticker" = ?
                  AND "Date" >= datetime(?, 'start of day')
                  AND "Date" <  datetime(?, '+1 day', 'start of day')
                ORDER BY "Date"; 
                """

            est_sql = """
                  SELECT "date","avg","low","high","yearAgoEps","numberOfAnalysts","growth","ticker"
                  FROM earnings_estimate
                  WHERE "ticker" = ?;
                  """

            rec_sql = """
              SELECT "date","period","strongBuy","buy","hold","sell","strongSell"
              FROM recommendations
              WHERE "ticker" = ?
              ORDER BY "date" DESC; 
              """

            results = {}
            with sqlite3.connect(DB_FILE) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()

                for i in range(0, len(tokens), 3):
                    ticker = tokens[i].strip().upper()
                    start = tokens[i + 1].strip()
                    end = tokens[i + 2].strip()

                    prices = [
                        dict(r)
                        for r in cur.execute(price_sql, (ticker, start, end)).fetchall()
                    ]
                    est_rows = cur.execute(est_sql, (ticker,)).fetchall()
                    estimates = [dict(r) for r in est_rows]
                    rec_rows = cur.execute(rec_sql, (ticker,)).fetchall()
                    recs = [dict(r) for r in rec_rows]
                    results[ticker] = {
                        "prices": prices,
                        "earnings_estimate": estimates,
                        "recommendations": recs,
                    }
            if len(str(results)) < 100:
                raise FileNotFoundError(results)

            return results
        except Exception as e:
            self.update_db(query)
            print("Updating DB..." + str(e))

    def update_db(self, query):
        output = self.thing2(query)
        engine = create_engine("sqlite:////Users/anvay/stockstats.db")
        conn = engine.connect()
        conn.execution_options(isolation_level="AUTOCOMMIT")
        for i in range(0, len(output), 3):
            ticker = output[i].strip().upper()
            df = yf.Ticker(ticker).earnings_estimate
            df = df.rename_axis("date").reset_index().assign(ticker=ticker)
            df.to_sql(
                name="earnings_estimate",
                method=None,
                schema=None,
                index=False,
                if_exists="append",
                con=engine,
            )
            df = yf.Ticker(ticker).history(period="max")
            df = df.rename_axis("date").reset_index().assign(ticker=ticker)
            df.to_sql(
                name="price_daily",
                method=None,
                schema=None,
                index=False,
                if_exists="append",
                con=engine,
            )
            df = yf.Ticker(ticker).get_recommendations()
            df = df.rename_axis("date").reset_index().assign(ticker=ticker)
            df.to_sql(
                name="recommendations",
                method=None,
                schema=None,
                index=False,
                if_exists="append",
                con=engine,
            )
            df = yf.Ticker(ticker).get_recommendations()
            df = df.rename_axis("date").reset_index().assign(ticker=ticker)
            df.to_sql(
                name="recommendations",
                method=None,
                schema=None,
                index=False,
                if_exists="append",
                con=engine,
            )
            df = yf.Ticker(ticker).get_upgrades_downgrades()
            df = df.rename_axis("date").reset_index().assign(ticker=ticker)
            df.to_sql(
                name="upgrade_downgrade",
                method=None,
                schema=None,
                index=False,
                if_exists="append",
                con=engine,
            )
        self.sql_query(query)

    def stock_analyzer_final(self, query):
        market_data = self.sql_query(query)
        client = OpenAI(api_key=OPENAI_API_KEY)
        #    datenow = datetime.datetime.now()
        #    datenow = datenow.strftime("%Y-%m-%d")
        client.containers.create(name="Quant-Container")
        prompt = f"""
        
        You are an expert data analyst working for Jane Street. I am in need of your help to analyze the following market data. Conduct a 
        market analysis for the specific industry or product given below in this string. Evaluate the current market 
        trends, competitive landscape, and consumer behavior. Summarize the key findings and suggest strategies for 
        market entry or growth. The History data from the stocks comes from {datenow} to exactly 12 months prior
        Include relevant statistics and examples. You MUST provide accurate data BASED on the data you have received. 
        Your output MUST be relevent to the query given and NO MORE than 700 words
        and nothing else.
        If you have lots of stock data, I want you to calculate things like RSI and different predictive factors 
        based on that plethora of data, though I also want to balance that with accurate predictions. If there isnt enough data
        to work with, just give a more basic breakdown instead of asking for more.
        If the user asks a simple prompt, or question, answer it cleanly and clearly.
        Here is the data I need you to analyze
        
        {market_data}

        """
        # noinspection PyTypeChecker
        resp = client.responses.create(
            model="gpt-5-mini",
            instructions=prompt,
            input=query,
            tools=[{"type": "code_interpreter", "container": {"type": "auto"}}],
            tool_choice="auto",
            parallel_tool_calls=True,
            reasoning={"summary": "auto"},
        )

        summary_text = None
        for item in getattr(resp, "output", []):
            if getattr(item, "type", None) == "reasoning":
                summaries = getattr(item, "summary", None)
                if isinstance(summaries, list) and summaries:
                    first = summaries[0]
                    summary_text = getattr(
                        first,
                        "text",
                        first.get("text") if isinstance(first, dict) else str(first),
                    )
                elif isinstance(summaries, str):
                    summary_text = summaries
                    break

        final = summary_text + "" + resp.output_text
        return final
