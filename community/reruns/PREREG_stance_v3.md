# Pre-registration: re-run exp8/9/10 with judge-v3 labels (written 2026-11-11, BEFORE running)

Change: post stance labels. Low-confidence posts (FinTwitBERT max prob < 0.5, 4,494 posts) take the
LLM judge v3 label (gate-passed 2026-11-11); all other posts keep the FinTwitBERT label.
Nothing else changes: same code, same data snapshot, same windows, same N-search rule.

Procedure:
1. Re-run all three experiments TODAY with the ORIGINAL labels (data has grown since the README numbers).
2. Re-run with the MERGED labels. Both outputs are saved and reported side by side.
3. No re-tuning, no re-picking N, no new hypotheses after seeing either output.

Decision rule (fixed now):
- An experiment's conclusion is "changed" ONLY IF, in the confirmation window (Sep 1 onward), the main
  test flips sign OR crosses p = 0.05 in either direction.
- Otherwise "unchanged", even if the numbers move.
- If N chosen by exp8's Jul-Aug search differs between label sets, report both, and score each on its own N.

Known limits (state in any write-up):
- Only 3.7% of posts (4,494 of ~121k) are replaced, all from the low-confidence slice.
- The judge was validated on 136 hand-labeled posts (bull/bear agreement 92% vs FinTwitBERT 62%), so label noise remains.
- This is a sensitivity check on label quality, not a new finding.
