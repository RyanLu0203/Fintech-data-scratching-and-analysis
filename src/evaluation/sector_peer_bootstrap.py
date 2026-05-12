"""Supplement local data for peer-sector NLP corpora without changing layout."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Callable

import pandas as pd

from src.config.paths import stock_data_dir
from src.data_ingestion.ingestion import IngestionConfig, run_ingestion
from src.nlp.peer_sentiment import (
    DATA_END_DATE,
    DATA_START_DATE,
    MIN_SECTOR_PEER_STOCKS,
    build_stock_sector_mapping,
)

LOGGER = logging.getLogger(__name__)


def ensure_sector_peer_data(
    *,
    target_symbols: list[str] | None = None,
    start_date: str = DATA_START_DATE,
    end_date: str = DATA_END_DATE,
    sources: str = "tencent",
    news_count: int = 5000,
    min_peer_stocks: int = MIN_SECTOR_PEER_STOCKS,
    allow_fetch: bool = False,
    fetch_all_configured_peers: bool = True,
    status_callback: Callable[[str, str], None] | None = None,
) -> dict[str, pd.DataFrame]:
    """Ensure configured same-sector peers have local integrated CSVs.

    The target is excluded later during corpus construction, so each target
    sector needs at least ``min_peer_stocks + 1`` locally available stocks.
    By default this function fetches every configured missing peer in the
    selected sectors, not just the minimum required count. That keeps the
    official peer cross analysis reproducible and avoids half-filled sectors.
    """

    def emit(stage: str, message: str) -> None:
        LOGGER.info("[%s] %s", stage, message)
        if status_callback:
            status_callback(stage, message)

    mapping = build_stock_sector_mapping()
    mapping["symbol"] = mapping["symbol"].astype(str).str.extract(r"(\d{6})", expand=False)
    target_set = {_normalize_symbol(symbol) for symbol in target_symbols or [] if str(symbol).strip()}
    if target_set:
        sectors = sorted(set(mapping.loc[mapping["symbol"].isin(target_set), "sector"].dropna().astype(str)))
    else:
        sectors = sorted(set(mapping.loc[mapping["is_target_candidate"].astype(str).isin(["1", "True", "true"]), "sector"].dropna().astype(str)))
    sectors = [sector for sector in sectors if sector and sector.upper() != "UNKNOWN"]

    rows: list[dict[str, object]] = []
    fetched: list[dict[str, object]] = []
    for sector in sectors:
        sector_rows = mapping[mapping["sector"].astype(str) == sector].copy()
        sector_rows = sector_rows.sort_values(["local_data_available", "news_count_2024_2026"], ascending=[False, False])
        required_total = min_peer_stocks + 1
        local_count = int(sector_rows["local_data_available"].astype(bool).sum())
        missing_rows = sector_rows[~sector_rows["local_data_available"].astype(bool)].copy()
        configured_count = int(len(sector_rows))
        missing_symbols = [str(value) for value in missing_rows["symbol"].dropna().astype(str).tolist()]
        emit(
            "sector_peer_check",
            f"{sector}: local={local_count}, configured={configured_count}, required_total={required_total}.",
        )

        if missing_symbols and (fetch_all_configured_peers or local_count < required_total) and allow_fetch:
            needed = configured_count if fetch_all_configured_peers else max(required_total - local_count, 0)
            rows_to_fetch = missing_rows if fetch_all_configured_peers else missing_rows.head(needed)
            for _, item in rows_to_fetch.iterrows():
                symbol = str(item["symbol"])
                company = str(item.get("company_name", "") or symbol)
                try:
                    emit("sector_peer_fetch", f"Fetching {symbol} {company} for sector {sector} ({start_date} to {end_date}).")
                    csv_path = run_ingestion(
                        IngestionConfig(
                            symbol=symbol,
                            company_name=company,
                            start_date=start_date,
                            end_date=end_date,
                            sources=sources,
                            news_count=news_count,
                            reuse_existing_csv=True,
                            require_news=False,
                            use_sqlite=False,
                        )
                    )
                    fetched.append({"symbol": symbol, "company_name": company, "sector": sector, "status": "fetched", "csv_path": str(csv_path)})
                    local_count += 1
                    if not fetch_all_configured_peers and local_count >= required_total:
                        break
                except Exception as exc:
                    fetched.append({"symbol": symbol, "company_name": company, "sector": sector, "status": "failed", "error": str(exc)})
                    emit("sector_peer_fetch_failed", f"{symbol} {company}: {exc}")
        elif missing_symbols:
            reason = "fetching disabled" if not allow_fetch else "minimum already met"
            for _, item in missing_rows.iterrows():
                fetched.append(
                    {
                        "symbol": str(item.get("symbol", "")),
                        "company_name": str(item.get("company_name", "")),
                        "sector": sector,
                        "status": "missing_not_fetched",
                        "reason": reason,
                    }
                )
            if local_count < required_total:
                emit("sector_peer_missing", f"{sector}: missing {required_total - local_count} required local peer stocks; {reason}.")
            elif fetch_all_configured_peers:
                emit("sector_peer_missing_optional", f"{sector}: required count is met but configured peers are still missing; {reason}.")

        rows.append(
            {
                "sector": sector,
                "required_total_stocks": required_total,
                "configured_sector_stock_count": configured_count,
                "local_stock_count_before_or_after": local_count,
                "status": "READY" if local_count >= required_total else "INSUFFICIENT",
                "missing_configured_symbols": ", ".join(missing_symbols),
                "fetch_all_configured_peers": fetch_all_configured_peers,
                "allow_fetch": allow_fetch,
            }
        )

    refreshed = build_stock_sector_mapping()
    diagnostics = pd.DataFrame(rows)
    fetched_df = pd.DataFrame(fetched)
    out_dir = Path("reports") / "tables"
    out_dir.mkdir(parents=True, exist_ok=True)
    diagnostics.to_csv(out_dir / "sector_peer_data_readiness.csv", index=False, encoding="utf-8-sig")
    fetched_df.to_csv(out_dir / "sector_peer_fetch_log.csv", index=False, encoding="utf-8-sig")
    return {"mapping": refreshed, "readiness": diagnostics, "fetch_log": fetched_df}


def _normalize_symbol(symbol: str) -> str:
    extracted = pd.Series([str(symbol)]).str.extract(r"(\d{6})", expand=False).iloc[0]
    return str(extracted) if pd.notna(extracted) else str(symbol).strip()
