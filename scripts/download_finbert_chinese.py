"""Download the verified immutable Chinese checkpoint; keep weights outside Git."""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess

MODEL = "yiyanghkust/finbert-tone-chinese"
REVISION = "e91b1a3af10e1e8c9c03429d3cd7d5e9a1c8000d"
WEIGHTS_SHA256 = "0c25b105902f90d1e039d511944af4f49d1490f6698e701f2704a714ca74e66b"

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--destination", type=Path, required=True)
    args = parser.parse_args()
    args.destination.mkdir(parents=True, exist_ok=True)
    files = [
        "config.json",
        "model.safetensors",
        "special_tokens_map.json",
        "tokenizer_config.json",
        "vocab.txt",
        "README.md",
    ]
    hashes = {}
    for name in files:
        path = args.destination / name
        subprocess.run(
            [
                "curl",
                "-fLsS",
                "--connect-timeout",
                "15",
                "--max-time",
                "600",
                "--retry",
                "2",
                "-o",
                str(path),
                f"https://huggingface.co/{MODEL}/resolve/{REVISION}/{name}",
            ],
            check=True,
        )
        hashes[name] = hashlib.sha256(path.read_bytes()).hexdigest()
        if name == "model.safetensors" and hashes[name] != WEIGHTS_SHA256:
            raise ValueError("Weight bytes differ from the validated checkpoint")
        print(f"downloaded {name} {path.stat().st_size} bytes", flush=True)
    (args.destination / "download_manifest.json").write_text(
        json.dumps({"model": MODEL, "revision": REVISION, "files": hashes}, indent=2)
    )
