# Week-ahead model ideas

The backlog for `/improve-model`. Each idea has a status:
- `todo`: untested
- `dev-win`: beat the champion on dev, not checked yet
- `rejected`: lost on dev, or failed its check
- `promoted`: passed its check and became the champion

Move an idea to `rejected` as soon as it fails its check. A variant of a rejected idea is a
new idea, and needs a clearer win on dev.

The champion scores 15.18 GBP/MWh overall. Even given the demand and wind that actually
happened, the model is about 12 out, so the price model is where most of the gain is (see
"Ceilings" below).

## Already tested (before the bench)

| Idea | Result | Status |
|---|---|---|
| Fit window of 14, 30, 45, 90 or 180 days instead of 60 | All worse overall (15.7-16.8 vs 15.7 for 60, without the blend) | rejected |
| Blend 50% of the last 7 days instead of 25% | 16.1 vs 15.2 | rejected |
| Blend 10-40% | 20-25% best in both halves of the history | promoted (25%) |
| Price = SAP x (a + b * residual demand), gas scaling the merit order | 15.6 vs 15.2, worse in 61% of months | rejected |

## Ceilings (perfect foresight, all issues)

| Given the actual... | Error |
|---|---|
| nothing (the champion) | 15.18 |
| SAP on the target day | 14.89 |
| residual demand | 12.75 |
| both | 12.42 |

A week-ahead gas forecast is worth at most about 0.3. Simple gas predictors (the TTF-SAP gap,
the day's TTF move, the last week's trend) explain under 5% of SAP's next 3-7 day moves.

## To test

| # | Idea | Why | Status |
|---|---|---|---|
| 1 | Scarcity term: residual demand above about 28-30 GW (hinge or squared) | The merit order steepens past 30 GW, and Nov 2026 lived there | todo |
| 2 | Weight recent days more in the 60-day fit (e.g. a 20-day half-life) | The level jumped 50% in six weeks; equal weights lag | todo |
| 3 | Fit on price minus k x SAP, or fix the gas coefficient from a longer window | The 60-day gas coefficient swung 0.4-1.6 as gas and demand rose together | todo |
| 4 | Correct the low bias of the weather wind on days 4-7 (up to -0.46 GW on day 7) | Weather-based wind runs low at long leads | todo |
| 5 | Weekday and bank holiday effects | Not in the model at all | todo |
| 6 | French or Dutch price, or net imports, from the last 7 days | GB is more coupled to Europe in 2026 (r 0.63 to 0.78 with FR) | todo |
| 7 | Half-hourly model averaged to the day (uses `issue.hh`) | The shape within the day changed (night prices rose most) | todo |
| 8 | Blend weight that rises with days ahead | The blend helped days 5-7 most | todo |
| 9 | Ensemble of the champion and one or two others | Errors of different models partly cancel | todo |
| 10 | Seasonal fit: add the same weeks a year earlier to the fit window | Winter conditions are thin in a 60-day window in autumn | todo |

## Data ideas (need pipeline work first; don't build these in the bench)

| Idea | Note |
|---|---|
| European gas storage (GIE AGSI+), LNG send-out, Norwegian flows | Matter for monthly or seasonal outlooks more than week-ahead |
| Recent weather from Open-Meteo's Historical Forecast API | On the lakehouse backlog; fills the days ERA5 hasn't reached |
