# Pre-registration: exp1 overnight re-test

Status: APPROVED by the owner on 2026-11-14 (primary = ex earnings night), BEFORE the primary metric was computed.
Drafted by Claude the same day. Bookkeeping note: the owner approved in chat first; this file's header was switched
from DRAFT to approved a few minutes AFTER the opening run because a rename command failed silently. The rule text
below is unchanged from the draft the owner approved.
Outcomes in the sealed window have NOT been computed for the primary metric below.

## What was registered before (2026-10-05)
"Judge the overnight comment effect (exp 1) on data collected after 2026-10-05 only." The metric, the
earnings handling and the pass rule were not written down. This draft pins them.

## Power check (counts only, 2026-11-14)
| window | filter | overnight blocks | dates | bursts |
|---|---|---|---|---|
| Sep 1 - Oct 4 (already seen) | ex earnings night | 344 | 23 | 68 |
| after Oct 5 (sealed) | all nights | 435 | 29 | 80 |
| after Oct 5 (sealed) | ex earnings night | 422 | 29 | 67 |
| after Oct 5 (sealed) | ex 3 days post-earnings | 395 | 29 | 59 |

- 13 earnings nights fall in the sealed window and all 13 were followed by a burst.
- The Sep window of about the same size gave a 95% CI of about +/-0.04 (+0.009 [-0.036, +0.042]).
  The Jul-Aug effect was +0.046, so a true effect of that size would sit near the edge of the CI:
  NOT CONFIRMED is quite possible even if the effect is real. A NOT CONFIRMED is weak evidence against;
  a CONFIRMED would be strong.

## Rule (fixed)
- Script: exp1_burst_hourly.py, "time split" scheme (model trained before 2026-09-01, unchanged).
- Sample: overnight blocks (15:30 bar -> next day's 9:30 bar) starting on or after 2026-10-05.
- PRIMARY: ex earnings night. Statistic = PR-AUC(trading + comments) - PR-AUC(trading),
  date-block bootstrap 95% CI, 1000 draws. CONFIRMED only if the CI lower bound > 0.
- SECONDARY (descriptive only): all nights, and ex 3 days post-earnings.
- No re-tuning, no new filters after opening.

## Why ex-earnings is primary
An earnings date is known from the calendar in advance. If comments only help on earnings nights,
a calendar flag would do the same job, so that is not evidence that comments carry information.

## Contamination disclosure
On 2026-11-14 a re-run of exp2_sentiment.py printed a pooled number for "Sep 1+ overnight, all nights"
(trading+comments minus trading), which mixes the already-seen September with the sealed window and
includes earnings nights. It is not the primary metric above, but it is a hint about the "all nights"
secondary. This is one reason the all-nights number is descriptive only.
