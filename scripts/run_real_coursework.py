"""Run the fixed 200-episode protocol against a verified frozen source snapshot."""

from pathlib import Path
import argparse
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import pandas as pd  # noqa: E402
import torch  # noqa: E402
from src.evaluation.coursework import CourseworkConfig, run_coursework  # noqa: E402


def verify_snapshot(directory: Path) -> dict:
    manifest = json.loads((directory / "source_manifest.json").read_text())
    for name, digest in manifest["file_sha256"].items():
        if hashlib.sha256((directory / name).read_bytes()).hexdigest() != digest:
            raise ValueError(f"Source CSV changed: {name}")
    for request in manifest["requests"]:
        if (
            hashlib.sha256((directory / request["file"]).read_bytes()).hexdigest()
            != request["sha256"]
        ):
            raise ValueError(f"Source response changed: {request['file']}")
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--target", choices=["002475", "601138"], required=True)
    parser.add_argument("--output-root", type=Path, default=ROOT / "outputs/runs")
    args = parser.parse_args()
    source = verify_snapshot(args.snapshot)
    torch.set_num_threads(1)
    config = CourseworkConfig(target_symbol=args.target)
    protocol = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "config": asdict(config),
        "peers": ["002241", "601231"],
        "source_manifest_sha256": hashlib.sha256(
            (args.snapshot / "source_manifest.json").read_bytes()
        ).hexdigest(),
        "source_snapshot": str(args.snapshot),
        "torch_threads": 1,
        "selection": "same fixed peers, folds, seeds and budgets for both targets; no test-return tuning",
    }
    args.output_root.mkdir(parents=True, exist_ok=True)
    protocol_path = args.output_root / f"protocol_{args.target}.json"
    if protocol_path.exists():
        raise ValueError(
            "A protocol already exists here; use a new output root to preserve the audit trail"
        )
    protocol_path.write_text(json.dumps(protocol, indent=2), encoding="utf-8")

    def read(symbol: str, kind: str) -> pd.DataFrame:
        return pd.read_csv(
            args.snapshot / f"{symbol}_{kind}.csv", dtype={"symbol": str}
        )

    manifest_path = run_coursework(
        read(args.target, "market"),
        read(args.target, "news"),
        pd.concat([read(s, "news") for s in protocol["peers"]]),
        pd.concat([read(s, "market") for s in protocol["peers"]]),
        config,
        args.output_root,
    )
    manifest = json.loads(manifest_path.read_text())
    manifest["source_snapshot"] = source
    manifest["source_manifest_sha256"] = protocol["source_manifest_sha256"]
    manifest["protocol_file"] = protocol_path.name
    manifest["runtime"] = {
        "torch": torch.__version__,
        "torch_threads": 1,
        "device": "cpu",
    }
    manifest["limitations"] = [
        "disclosure titles, not complete media news",
        "date-only disclosure availability delayed to following calendar day",
        "retrospective qfq price snapshot; adjusted share units and dividend treatment are approximations",
        "simplified integer-share execution excludes A-share lots, T+1 sellability, limits, slippage and stamp-tax asymmetry",
        "upstream sentiment uses weak lexicon labels; external FinBERT evaluation is separate",
    ]
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(manifest_path, flush=True)
