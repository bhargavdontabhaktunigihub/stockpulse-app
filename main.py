"""
FastAPI Server for Stock Evaluator & Due Diligence App
"""
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import os

from data_fetcher import get_stock_data, get_fear_and_greed_index
from engine import evaluate_stock

app = FastAPI(title="Stock Pulse Evaluator API", version="1.0.0")

# Enable CORS for local and web testing
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
os.makedirs(STATIC_DIR, exist_ok=True)

@app.get("/api/evaluate/{ticker}")
async def evaluate_ticker_endpoint(ticker: str):
    """
    Fetches real-time market data and executes the 100-point fundamental algorithm.
    """
    clean_ticker = ticker.strip().upper()
    if not clean_ticker or len(clean_ticker) > 10:
        raise HTTPException(status_code=400, detail="Invalid ticker symbol format.")
        
    try:
        data = get_stock_data(clean_ticker)
        evaluation = evaluate_stock(data["metrics"])
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unable to process ticker '{clean_ticker}': {str(e)}")
        
    return {
        "evaluation": evaluation,
        "history": data["history"],
        "annual_returns": data.get("annual_returns", []),
        "insider_activity": data.get("insider_activity", {}),
        "earnings_data": data.get("earnings_data", {}),
        "market_sentiment": data.get("market_sentiment", {}),
        "analyst_targets": data.get("analyst_targets", {}),
        "bull_bear_cases": data.get("bull_bear_cases", {}),
        "dividend_data": data.get("dividend_data", {})
    }

@app.get("/api/market-sentiment")
async def market_sentiment_endpoint():
    """
    Returns live CNN Fear & Greed index and historical sentiment levels.
    """
    return get_fear_and_greed_index()

@app.get("/api/presets")
async def get_presets():
    """
    Returns featured preset tickers with live baseline ratings.
    """
    return [
        {"ticker": "NVDA", "name": "NVIDIA Corp.", "category": "AI / Semiconductors", "tag": "High Growth"},
        {"ticker": "GOOGL", "name": "Alphabet Inc.", "category": "Platforms / Cloud", "tag": "Value Compounder"},
        {"ticker": "VOO", "name": "Vanguard S&P 500", "category": "Broad Index ETF", "tag": "Core Anchor"},
        {"ticker": "AAPL", "name": "Apple Inc.", "category": "Consumer Tech", "tag": "Brand Moat"},
        {"ticker": "MSFT", "name": "Microsoft Corp.", "category": "Enterprise Cloud & AI", "tag": "Software Moat"},
        {"ticker": "AMZN", "name": "Amazon.com Inc.", "category": "E-Commerce & AWS", "tag": "Ecosystem"}
    ]

# Mount static directory and index.html
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

@app.get("/")
async def root():
    index_file = os.path.join(STATIC_DIR, "index.html")
    if os.path.exists(index_file):
        return FileResponse(index_file)
    return {"message": "Stock Evaluator Backend running. static/index.html is being prepared."}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
