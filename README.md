# StockPulse | Real-Time Stock Due Diligence Evaluator

StockPulse is a modern, responsive web application that takes any stock ticker symbol (e.g. `NVDA`, `GOOGL`, `VOO`, `AAPL`, `MSFT`), retrieves live market fundamentals, and executes an automated **100-point 4-pillar algorithmic evaluation engine** to output a definitive verdict:

- **Strong Buy / High Conviction** (80–100)
- **Buy / Accumulate** (65–79)
- **Hold / Neutral** (45–64)
- **Caution / Overvalued** (30–44)
- **High Risk / Avoid** (<30)

---

## Architecture Overview

- **Backend**: FastAPI (Python 3.9+) with asynchronous endpoints for live ticker ingestion and scoring.
- **Data Engine**: `data_fetcher.py` using `yfinance` with an in-memory TTL cache and fallback datasets.
- **Scoring Engine**: `engine.py` evaluating:
  1. **Valuation & Margin of Safety** (30 pts max)
  2. **Balance Sheet & Solvency** (25 pts max)
  3. **Profitability & Moat Quality** (25 pts max)
  4. **Growth Velocity** (20 pts max)
- **Frontend**: Responsive modern single-page dashboard using TailwindCSS, Lucide Icons, and Chart.js for 1-year historical price and valuation trajectories.

---

## How to Run

From this directory:
```bash
./run.sh
```

Or manually using the local virtual environment:
```bash
source venv/bin/activate
uvicorn main:app --host 127.0.0.1 --port 8000 --reload
```

Then open your browser to:
**http://127.0.0.1:8000**
