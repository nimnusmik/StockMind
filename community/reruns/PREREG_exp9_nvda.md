# Pre-registration: exp9 NVDA 5-day bounce re-test

Status: written 2026-11-14 BEFORE computing any outcome on the sealed sample. The owner asked for the seal
to be opened ("do 1 and 2") after the rule for the exp1 overnight seal had been pinned the same way.

## What was registered before (2026-10-05, exp9_drop_vs_comments.py docstring)
"Judge 'NVDA 5-day excess after signals > similar-drop days' using ONLY data collected after 2026-10-05."
The pass rule was not written down. This file pins it.

## Rule (fixed)
- Script machinery: exp9_drop_vs_comments.py unchanged (signal = bearish tilt + heat, N = 1; drop size =
  3-day cumulative excess return, quintiles within the ticker over the whole sample; effect = signal-day mean
  minus non-signal-day mean within the same drop quintile, weighted by signal-day counts).
- Sample: NVDA ticker-days with date > 2026-10-05 and a complete next-5-day outcome.
- PRIMARY: next-5-day cumulative excess return, drop-matched difference, date-block bootstrap 95% CI
  (2000 draws). CONFIRMED only if the CI lower bound > 0.
- SECONDARY (descriptive only): the same on all 15 tickers pooled after 2026-10-05; NVDA next-day.
- No re-tuning, no new filters after opening.

## Known limits
- 5-day outcomes of consecutive days overlap, so the effective sample is smaller than the day count.
- The window contains one sharp NVDA sell-off (Oct 28 to Nov 13). A single regime cannot settle the question.

## Power check BEFORE opening (2026-11-14, counts only)
NVDA after Oct 5: 24 days (10-06 to 11-06), 10 signal days. By drop quintile (non-signal / signal):
bin 0: 2 / 6, bin 1: 3 / 3, bin 2: 5 / 1, bin 3: 4 / 0. Six of the ten signal days are matched against only
two non-signal days, and 5-day outcomes overlap. Written before opening: the CI will be very wide, so
NOT CONFIRMED is the likely outcome even if the effect is real. A NOT CONFIRMED here is weak evidence
against; a CONFIRMED would be notable. 15 tickers pooled: 360 ticker-days on 24 dates, 75 signal days.
