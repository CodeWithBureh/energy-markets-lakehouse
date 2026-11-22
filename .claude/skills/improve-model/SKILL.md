---
name: improve-model
description: Run a round of experiments to improve the week-ahead GB power price model with the research bench (research/bench.py) - refresh the snapshot, test ideas from research/ideas.md on dev, check the best against the holdout, and open a PR if one is promoted. Use when asked to improve, tune or research the week-ahead price model or report.
---

# Improve the week-ahead price model

Each round tests a few ideas against the champion and logs every result. A model only changes
if it passes the bench's gates on data it was never tuned on. Read `research/README.md` first.

Arguments (optional): the number of ideas to test (default 3), or specific idea numbers from
`research/ideas.md`.

## Steps

1. **Snapshot.** If `research/data/meta.json` is missing or more than 7 days old, run
   `python -m research.export` (warehouse and profile from `DATABRICKS_WAREHOUSE_ID` and
   `DATABRICKS_CONFIG_PROFILE`; ask the user if they're unset). This is read-only. Don't run
   any job, backfill or pipeline.
2. **Catch up.** Read `research/champion.json`, `research/ideas.md` and
   `python -m research.bench log`. Don't repeat anything already tested.
3. **Pick ideas.** Choose `todo` ideas, most promising first: the ceilings in `ideas.md` say
   where the error is. You may add a new idea to the table when the results suggest one, with
   a one-line reason.
4. **Build each idea** as `research/candidates/<short_name>.py`:
   - a docstring saying what it does;
   - `DESCRIPTION = __doc__`;
   - `predict(issue)`.

   Keep it as simple as the idea allows, and reuse `research/lib.py` and
   `report_blend.estimate`. Use only what `issue` gives you: never read `research/data/`
   files directly, or anything dated on or after `issue.issue_date`.
5. **Score on dev:** `python -m research.bench run <name> --note "<what and why>"`.
   - You may adjust an idea and rerun on dev; each run is logged.
   - Settle each setting by reasoning, or from a small grid fixed in advance. Don't search
     wide grids for the best number.
6. **Check at most once per idea.** Run `python -m research.bench check <name>` only when the
   dev result is a clear win:
   - lower in both dev halves;
   - at least 0.15 GBP/MWh lower overall;
   - month win share above 0.55.

   Never change an idea after seeing its check and check it again. A failed check means
   `rejected`.
7. **Record.** Update the idea's status in `research/ideas.md` with its dev and holdout error.
8. **If a check passes:**
   - Run `python -m research.bench promote <name>`.
   - Port the model to `sql/week_ahead_report.sql` and `sql/week_ahead_backtest.sql`, keeping
     the params CTE style.
   - Run the backtest SQL read-only on prod, with `research.export.Warehouse(...).query`. It
     must reproduce the bench's error to within 0.1 for each day ahead, or the port is wrong.
9. **Open one PR** against `dev` from a new `feature/model-...` branch with:
   - the candidates;
   - `experiments.jsonl`, `ideas.md` and `champion.json`;
   - any SQL changes.

   In the description, give a table of every idea tried this round (dev, holdout, verdict).
   Never enable auto-merge. End it with the repo's attribution line. If nothing was promoted,
   the PR records the round's results only.
10. **Report back:**
    - what was tried;
    - what won or lost, with numbers;
    - what you'd try next.

## Rules

- The holdout is only for `check`. Don't compute holdout errors any other way, or select or
  tune on them.
- If the holdout is shorter than 60 issues, or a check would compare fewer than 300 rows, stop
  and say so rather than promote.
- Don't change `research/bench.py`'s gates, split dates or scoring in the same PR as a
  candidate. Propose those separately, with a reason.
- Data that the lakehouse doesn't have yet goes in the "Data ideas" table, not in a candidate.
- Stop after the agreed number of ideas, or sooner after three dev losses in a row.
