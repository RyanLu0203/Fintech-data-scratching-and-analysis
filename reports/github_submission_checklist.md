# GitHub Submission Checklist

- `.git` exists: `False`
- Do not push automatically from this script.
- Keep `.venv/`, `outputs/`, cache folders, SQLite databases, logs, and large model files out of git.
- Include `README.md`, `requirements.txt`, `src/`, `program/`, `docs/`, `notebooks/`, `tests/`, and final report materials.
- Include only small sample outputs if needed for the demo; keep large generated artifacts outside the repo or in a release/archive.

## Recommended Commands

```bash
git init
git add README.md requirements.txt .gitignore src program docs notebooks tests reports/final_report.md reports/presentation_outline.md reports/live_demo_script.md reports/q_and_a_preparation.md
git status
git commit -m "Prepare final NLP-RL trading platform submission"
git remote add origin <your-repo-url>
git push -u origin main
```