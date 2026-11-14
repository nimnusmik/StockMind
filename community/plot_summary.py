"""Summary figures for the README.

1. figures/summary_forest.png   every headline effect with its 95% CI on one page (forest plot).
   Numbers are copied from the recorded outputs in reruns/ (source noted per row); nothing is recomputed here.
2. figures/permutation_null.png the global permutation tests of experiments 12b, 13, 14: the null distribution
   of the scan maximum vs the observed maximum. Re-runs the three scripts (a few minutes).

Run: python3 plot_summary.py
"""
import contextlib
import io
import runpy
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.transforms import blended_transform_factory
import numpy as np

HERE = Path(__file__).parent
INK, INK2, GRID, SURF, DOT = "#0b0b0b", "#52514e", "#e6e5e1", "#fcfcfb", "#2a78d6"
plt.rcParams.update({"font.family": "DejaVu Sans", "axes.spines.top": False, "axes.spines.right": False,
                     "axes.spines.left": False, "figure.facecolor": SURF, "axes.facecolor": SURF})

# (label, n, estimate, lo, hi)  -- sources: reruns/20261114/*.txt, reruns/*_confirm_output.txt, reruns/overnight_posthoc_output.txt
GAIN = [
    ("Volume burst, all hours: + comments and sentiment (exp 2)", "5,550 blocks", -0.009, -0.034, 0.016),
    ("Volume burst, OVERNIGHT: + comments  [pre-registered re-test] (exp 1)", "422 blocks", 0.096, 0.019, 0.185),
    ("   same, after adding the overnight price gap  [post-hoc]", "422 blocks", 0.053, -0.007, 0.116),
    ("Price direction, next hour: + comments and sentiment (exp 2)", "5,516 blocks", 0.010, -0.009, 0.030),
    ("Trend reversal, next day: + sentiment (exp 7)", "769 days", -0.008, -0.034, 0.019),
    ("Direction, next day: excess sentiment vs coin flip (exp 4)", "769 days", -0.051, -0.093, -0.008),
    ("Direction, next day: all 18 features vs coin flip (exp 5)", "769 days", 0.024, -0.019, 0.070),
]
RET = [
    ("Bearish tilt + heat vs similar-drop days, next day (exp 9)", "139 signals", -0.03, -0.64, 0.62),
    ("   NVDA, next 5 days  [pre-registered re-test] (exp 9)", "10 signals", -3.90, -8.28, 3.78),
    ("Most toxic fifth of falling days vs rest, next 5 days (exp 10)", "409 days", 0.36, -1.08, 1.77),
    ("Most bearish fifth vs rest, next 5 days (exp 10)", "409 days", -0.68, -2.10, 0.76),
    ("Hottest fifth (post count) vs rest, next 5 days (exp 10)", "409 days", 1.34, -0.35, 3.04),
    ("Most surrender language vs rest, next 5 days (exp 11)", "409 days", -0.59, -2.30, 0.98),
    ("Accumulated rage, day 8  [pre-registered re-test] (exp 12)", "82 days", 1.40, -1.77, 5.26),
    ("   day 9  [pre-registered re-test]", "77 days", 0.53, -3.23, 5.20),
    ("   day 10  [pre-registered re-test]", "71 days", 0.15, -4.19, 5.84),
]


def forest(ax, rows, title, unit, fmt):
    for i, (label, n, est, lo, hi) in enumerate(rows):
        y = len(rows) - 1 - i
        out = lo > 0 or hi < 0  # CI excludes the null -> filled marker (shape, not colour, carries it)
        ax.plot([lo, hi], [y, y], color=DOT, lw=2, solid_capstyle="round")
        ax.plot(est, y, "o", ms=9, mfc=DOT if out else SURF, mec=DOT, mew=2, zorder=3)
        ax.text(-0.01, y, label, transform=ax.get_yaxis_transform(), ha="right", va="center", fontsize=9.5,
                color=INK, fontweight="bold" if "pre-registered" in label else "normal")
        ax.text(1.02, y, n, transform=ax.get_yaxis_transform(), ha="left", va="center", fontsize=9, color=INK2)
        ax.text(1.36, y, f"{est:{fmt}}  [{lo:{fmt}}, {hi:{fmt}}]", transform=ax.get_yaxis_transform(), ha="left",
                va="center", fontsize=9, color=INK, family="DejaVu Sans Mono")
    ax.axvline(0, color=INK2, lw=1, ls=(0, (3, 3)))
    ax.set_ylim(-0.7, len(rows) - 0.3)
    ax.set_yticks([])
    ax.grid(axis="x", color=GRID, lw=1)
    ax.set_axisbelow(True)
    ax.tick_params(colors=INK2, labelsize=9, length=0)
    ax.spines["bottom"].set_color(GRID)
    ax.set_xlabel(unit, color=INK2, fontsize=9.5)
    ax.text(0.02, 1.07, title, transform=blended_transform_factory(ax.figure.transFigure, ax.transAxes), fontsize=11.5,
            color=INK, fontweight="bold", va="bottom")
    top = len(rows) - 0.45
    ax.text(1.02, top, "n", transform=ax.get_yaxis_transform(), fontsize=9, color=INK2, va="bottom")
    ax.text(1.36, top, "estimate  [95% CI]", transform=ax.get_yaxis_transform(), fontsize=9, color=INK2, va="bottom")


fig, axes = plt.subplots(2, 1, figsize=(13, 8.6), gridspec_kw={"height_ratios": [len(GAIN), len(RET)], "hspace": 0.55})
fig.subplots_adjust(left=0.46, right=0.69, top=0.84, bottom=0.08)
forest(axes[0], GAIN, "A. What do comments add to a forecast?", "gain in PR-AUC or ROC-AUC (0 = adds nothing)", "+.3f")
forest(axes[1], RET, "B. Does the crowd mark the bottom?", "difference in excess return, percentage points (0 = no difference)", "+.2f")
fig.suptitle("Sixteen headline effects, one page: almost every interval contains zero", x=0.02, ha="left", fontsize=15,
             color=INK, fontweight="bold")
fig.text(0.02, 0.905, "Out-of-sample estimates with date-block bootstrap 95% CIs (tickers move together on the same day).\n"
         "Filled dot = interval excludes zero, hollow = contains zero. Bold = judged on data collected after the hypothesis was registered.",
         ha="left", fontsize=9.5, color=INK2)
fig.savefig(HERE / "figures" / "summary_forest.png", dpi=160)
print("saved figures/summary_forest.png")

# 2) permutation nulls: run the three scripts quietly and take their observed max and null maxima
fig, axes = plt.subplots(1, 3, figsize=(13, 3.9), sharey=False)
fig.subplots_adjust(left=0.05, right=0.98, top=0.68, bottom=0.17, wspace=0.22)
for ax, (script, title) in zip(axes, [("exp12b_permutation.py", "Exp 12: accumulated rage (20 cells)"),
                                      ("exp13_silence.py", "Exp 13: silence after rage (10 cells)"),
                                      ("exp14_meme.py", "Exp 14: meme tickers only (20 cells)")]):
    with contextlib.redirect_stdout(io.StringIO()):
        ns = runpy.run_path(str(HERE / script))
    null, obs = np.asarray(ns["maxes"]) * 100, ns["obs"] * 100
    p = (null >= obs).mean()
    ax.hist(null, bins=30, color=DOT, alpha=0.35, edgecolor=SURF, lw=1)
    ax.axvline(obs, color=INK, lw=2)
    ax.axvline(np.percentile(null, 95), color=INK2, lw=1, ls=(0, (3, 3)))
    ymax = ax.get_ylim()[1]
    ax.text(obs, ymax * 1.02, f"observed {obs:+.2f}pp", ha="center", va="bottom", fontsize=9, color=INK, fontweight="bold")
    ax.text(np.percentile(null, 95), ymax * 0.80, " 95th pct\n of the null", ha="left", va="top", fontsize=8.5, color=INK2)
    ax.set_title(f"{title}\nglobal p = {p:.3f}  ({len(null)} date permutations)", loc="left", fontsize=10.5, color=INK, pad=18)
    ax.set_xlabel("largest top-vs-rest difference in the whole scan (pp)", color=INK2, fontsize=9)
    ax.set_yticks([])
    ax.tick_params(colors=INK2, labelsize=9, length=0)
    ax.spines["bottom"].set_color(GRID)
    print(f"{script}: observed {obs:+.2f}pp, p = {p:.3f}")
fig.suptitle("Scanning many cells finds something by chance: the observed maximum sits inside the no-effect distribution",
             x=0.02, y=0.985, ha="left", fontsize=13.5, color=INK, fontweight="bold")
fig.text(0.02, 0.875, "Histogram = the largest cell a scan of the same size produces when dates are shuffled (no real relation). "
         "Black line = what the real data produced.", ha="left", fontsize=9.5, color=INK2)
fig.savefig(HERE / "figures" / "permutation_null.png", dpi=160)
print("saved figures/permutation_null.png")
