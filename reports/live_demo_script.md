# Live Demo Script

Use cached data by default. Do not require live scraping during presentation.

1. Start dashboard: `python main.py --dashboard`.
2. Select a stock with existing outputs, such as `002475`, `300750`, or `600519`.
3. Keep `Reuse cached CSV / master slice` enabled.
4. Show preflight/audit status and explain data coverage.
5. Open sentiment trend, news count, portfolio curves, ablation metrics, and diagnostics.
6. Explain FinBERT status honestly: actual run when available, fallback when skipped.
7. Show cross-stock reliability status and common-window diagnostics.

Fallback plan: use screenshots, precomputed outputs, and dashboard bundles under `outputs/system/dashboard_exports/`.