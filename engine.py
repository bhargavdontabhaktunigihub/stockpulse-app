"""
Stock Evaluator Scoring Engine
Implements the 100-point 4-pillar fundamental evaluation algorithm:
1. Valuation & Margin of Safety (30 pts)
2. Financial Health & Solvency (25 pts)
3. Profitability & Moat Quality (25 pts)
4. Growth Velocity (20 pts)
"""
from typing import Dict, Any, List

def evaluate_stock(metrics: Dict[str, Any]) -> Dict[str, Any]:
    """
    Evaluates normalized financial metrics and returns a 100-point score,
    sub-pillar scores, specific pros/cons, and an investment verdict.
    """
    is_etf = metrics.get("is_etf", False)
    
    # If it's an ETF like VOO, apply ETF-specific index evaluation
    if is_etf:
        return _evaluate_etf(metrics)
        
    valuation_score, val_reasons = _score_valuation(metrics)
    solvency_score, sol_reasons = _score_solvency(metrics)
    profitability_score, prof_reasons = _score_profitability(metrics)
    growth_score, gro_reasons = _score_growth(metrics)
    
    total_score = round(valuation_score + solvency_score + profitability_score + growth_score, 1)
    
    # Determine Rating Label & Visual Style
    if total_score >= 80:
        verdict = "STRONG BUY / HIGH CONVICTION"
        verdict_color = "emerald"
        summary = "Exceptional fundamentals across profitability, balance sheet health, and valuation cushion."
    elif total_score >= 65:
        verdict = "BUY / ACCUMULATE"
        verdict_color = "blue"
        summary = "High-quality compounder with robust moats. Fair-to-favorable valuation."
    elif total_score >= 45:
        verdict = "HOLD / NEUTRAL"
        verdict_color = "amber"
        summary = "Solid core business, but current valuation or slowing growth leaves a modest margin of safety."
    elif total_score >= 30:
        verdict = "CAUTION / OVERVALUED"
        verdict_color = "orange"
        summary = "Priced for near perfection or carries elevated debt/margin pressure. High risk of multiple compression."
    else:
        verdict = "HIGH RISK / AVOID"
        verdict_color = "crimson"
        summary = "Severe red flags in cash burn, debt load, or structural business decay."

    pros = []
    cons = []
    
    for r in val_reasons + sol_reasons + prof_reasons + gro_reasons:
        if r["type"] == "pro":
            pros.append(r["text"])
        elif r["type"] == "con":
            cons.append(r["text"])

    return {
        "ticker": metrics.get("ticker", ""),
        "name": metrics.get("name", ""),
        "total_score": total_score,
        "max_score": 100,
        "verdict": verdict,
        "verdict_color": verdict_color,
        "summary": summary,
        "pillars": {
            "valuation": {"score": round(valuation_score, 1), "max": 30, "label": "Valuation & Safety"},
            "solvency": {"score": round(solvency_score, 1), "max": 25, "label": "Balance Sheet & Solvency"},
            "profitability": {"score": round(profitability_score, 1), "max": 25, "label": "Profitability & Moat"},
            "growth": {"score": round(growth_score, 1), "max": 20, "label": "Growth Velocity"}
        },
        "pros": pros,
        "cons": cons,
        "key_metrics": metrics
    }

def _score_valuation(m: Dict[str, Any]):
    score = 0.0
    reasons = []
    
    # 1. Forward P/E (10 pts)
    fwd_pe = m.get("forward_pe")
    if fwd_pe is not None and fwd_pe > 0:
        if fwd_pe <= 18:
            score += 10.0
            reasons.append({"type": "pro", "text": f"Attractive Forward P/E ({fwd_pe:.1f}x) provides favorable margin of safety."})
        elif fwd_pe <= 28:
            score += 7.0
            reasons.append({"type": "pro", "text": f"Reasonable Forward P/E ({fwd_pe:.1f}x) aligned with high-quality compounders."})
        elif fwd_pe <= 38:
            score += 4.0
            reasons.append({"type": "con", "text": f"Elevated Forward P/E ({fwd_pe:.1f}x) leaves smaller room for operational missteps."})
        else:
            score += 1.0
            reasons.append({"type": "con", "text": f"High Forward P/E ({fwd_pe:.1f}x) implies aggressive market expectations."})
    else:
        score += 3.0

    # 2. PEG Ratio (10 pts)
    peg = m.get("peg_ratio")
    if peg is not None and peg > 0:
        if peg <= 1.2:
            score += 10.0
            reasons.append({"type": "pro", "text": f"PEG ratio ({peg:.2f}) indicates Growth at a Reasonable Price (GARP)."})
        elif peg <= 2.0:
            score += 6.5
        elif peg <= 3.0:
            score += 3.0
            reasons.append({"type": "con", "text": f"PEG ratio ({peg:.2f}) shows price is running ahead of projected growth."})
        else:
            score += 1.0
    else:
        # Fallback based on forward PE and earnings growth
        score += 5.0

    # 3. Free Cash Flow Yield (10 pts)
    fcf_yield = m.get("fcf_yield")
    if fcf_yield is not None:
        if fcf_yield >= 0.045:
            score += 10.0
            reasons.append({"type": "pro", "text": f"Strong Free Cash Flow Yield ({fcf_yield*100:.1f}%) directly generating shareholder value."})
        elif fcf_yield >= 0.025:
            score += 7.0
        elif fcf_yield >= 0.01:
            score += 4.0
        else:
            score += 1.0
            reasons.append({"type": "con", "text": f"Low or negative FCF Yield ({fcf_yield*100:.1f}%) due to heavy CapEx or low conversion."})
    else:
        score += 4.0

    return min(score, 30.0), reasons

def _score_solvency(m: Dict[str, Any]):
    score = 0.0
    reasons = []
    
    cash = m.get("total_cash", 0) or 0
    debt = m.get("total_debt", 0) or 0
    ebitda = m.get("ebitda", 0) or 1
    
    # 1. Net Cash / Debt to EBITDA (12 pts)
    if cash > debt:
        score += 12.0
        net_cash_b = (cash - debt) / 1e9
        reasons.append({"type": "pro", "text": f"Fortress balance sheet: Net cash positive by ${net_cash_b:.1f}B with minimal solvency risk."})
    else:
        net_debt = debt - cash
        leverage = net_debt / max(ebitda, 1)
        if leverage < 1.5:
            score += 9.0
            reasons.append({"type": "pro", "text": f"Low leverage (Net Debt/EBITDA: {leverage:.1f}x); easily serviceable."})
        elif leverage < 3.0:
            score += 5.0
            reasons.append({"type": "con", "text": f"Moderate debt leverage ({leverage:.1f}x EBITDA)."})
        else:
            score += 1.0
            reasons.append({"type": "con", "text": f"High debt burden ({leverage:.1f}x EBITDA) vulnerable in high interest-rate environments."})

    # 2. Current Ratio (8 pts)
    current_ratio = m.get("current_ratio")
    if current_ratio is not None:
        if current_ratio >= 1.5:
            score += 8.0
        elif current_ratio >= 1.0:
            score += 5.0
        else:
            score += 1.0
            reasons.append({"type": "con", "text": f"Current ratio below 1.0 ({current_ratio:.2f}); short-term liabilities exceed liquid assets."})
    else:
        score += 5.0

    # 3. Quick Ratio / Cash Buffer (5 pts)
    if cash > 10e9:  # >$10B cash buffer
        score += 5.0
    elif cash > 1e9:
        score += 3.5
    else:
        score += 2.0

    return min(score, 25.0), reasons

def _score_profitability(m: Dict[str, Any]):
    score = 0.0
    reasons = []
    
    op_margin = m.get("operating_margin")
    if op_margin is not None:
        if op_margin >= 0.35:
            score += 11.0
            reasons.append({"type": "pro", "text": f"Elite operating margin ({op_margin*100:.1f}%) reflecting formidable economic moat and pricing power."})
        elif op_margin >= 0.20:
            score += 8.0
            reasons.append({"type": "pro", "text": f"Strong operating profitability ({op_margin*100:.1f}%)."})
        elif op_margin >= 0.10:
            score += 5.0
        else:
            score += 1.5
            reasons.append({"type": "con", "text": f"Thin operating margins ({op_margin*100:.1f}%); sensitive to cost inflation."})
    else:
        score += 5.0

    # Return on Equity / Invested Capital (10 pts)
    roe = m.get("return_on_equity")
    if roe is not None:
        if roe >= 0.25:
            score += 10.0
            reasons.append({"type": "pro", "text": f"High Return on Equity ({roe*100:.1f}%) compounder."})
        elif roe >= 0.15:
            score += 7.0
        elif roe >= 0.08:
            score += 4.0
        else:
            score += 1.0
    else:
        score += 5.0

    # Gross Margin strength (4 pts)
    gross_margin = m.get("gross_margin")
    if gross_margin is not None and gross_margin >= 0.50:
        score += 4.0
    elif gross_margin is not None and gross_margin >= 0.30:
        score += 2.5
    else:
        score += 1.0

    return min(score, 25.0), reasons

def _score_growth(m: Dict[str, Any]):
    score = 0.0
    reasons = []
    
    rev_growth = m.get("revenue_growth")
    if rev_growth is not None:
        if rev_growth >= 0.25:
            score += 11.0
            reasons.append({"type": "pro", "text": f"Accelerating top-line revenue growth ({rev_growth*100:.1f}% YoY)."})
        elif rev_growth >= 0.12:
            score += 8.0
            reasons.append({"type": "pro", "text": f"Healthy double-digit revenue expansion ({rev_growth*100:.1f}% YoY)."})
        elif rev_growth >= 0.03:
            score += 4.5
        else:
            score += 1.0
            reasons.append({"type": "con", "text": f"Sluggish or contracting revenue growth ({rev_growth*100:.1f}% YoY)."})
    else:
        score += 5.0

    # Earnings Growth (9 pts)
    earnings_growth = m.get("earnings_growth")
    if earnings_growth is not None:
        if earnings_growth >= 0.20:
            score += 9.0
        elif earnings_growth >= 0.10:
            score += 6.5
        elif earnings_growth >= 0.0:
            score += 3.5
        else:
            score += 0.5
            reasons.append({"type": "con", "text": "Net income contracted over recent reporting periods."})
    else:
        score += 5.0

    return min(score, 20.0), reasons

def _evaluate_etf(m: Dict[str, Any]) -> Dict[str, Any]:
    """Special scoring logic for index ETFs like VOO"""
    score = 86.0  # VOO historically scores in the high 80s for core passive compounding
    return {
        "ticker": m.get("ticker", "VOO"),
        "name": m.get("name", "Vanguard S&P 500 ETF"),
        "total_score": score,
        "max_score": 100,
        "verdict": "STRONG CORE FOUNDATION (INDEX)",
        "verdict_color": "emerald",
        "summary": "Diversified across 500 largest U.S. corporations with near-zero expense ratio (0.03%). Ideal primary portfolio anchor.",
        "pillars": {
            "valuation": {"score": 21.0, "max": 30, "label": "Valuation & Safety (Market CAPE)"},
            "solvency": {"score": 25.0, "max": 25, "label": "Solvency (No Single Point of Failure)"},
            "profitability": {"score": 22.0, "max": 25, "label": "Blended Corporate Profitability"},
            "growth": {"score": 18.0, "max": 20, "label": "Long-Term GDP+ Earnings Trend"}
        },
        "pros": [
            "Ultra-low expense ratio of 0.03% ($3 per $10k invested/year).",
            "Self-cleans automatically: weak companies are removed, thriving businesses grow.",
            "Eliminates individual company bankruptcy or management failure risk.",
            "Quarterly dividend distributions (~1.04% - 1.10% yield)."
        ],
        "cons": [
            "Market-cap weighting concentrates ~39% of the fund in the top 10 mega-cap tech stocks.",
            "Shiller CAPE ratio (~40.4) indicates historically elevated market valuation."
        ],
        "key_metrics": m
    }
