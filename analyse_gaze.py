"""
Analyse gaze data from the reading experiment.
================================================
Usage:  python analyse_gaze.py results.json [screen_w] [screen_h]

Epochs gaze data using jsPsych time_elapsed aligned with local_ts
(both use performance.now() so share the same clock).
"""

import json
import sys
import numpy as np

# ── Parameters ────────────────────────────────────────────────────
SCREEN_W = 1920
SCREEN_H = 1080
FONT_SIZE_PX = 64
CHAR_WIDTH_PX = 38.4  # Courier New 64px ≈ 0.6 × font-size


def load_results(path):
    with open(path) as f:
        data = json.load(f)
    gaze = data["gaze"]
    trials = json.loads(data["jspsych"])
    return gaze, trials


def estimate_clock_offset(gaze, trials):
    """
    Both local_ts and time_elapsed derive from performance.now().
    local_ts = performance.now()  (absolute)
    time_elapsed = performance.now() - jsPsych_start  (relative)
    So: jsPsych_start = local_ts - time_elapsed

    We estimate jsPsych_start using the first gaze sample (which arrives
    right after the call-function trial that opens the WebSocket).
    """
    connect_trial = next(t for t in trials if t["trial_type"] == "call-function")
    first_gaze_ts = gaze[0]["local_ts"]
    offset = first_gaze_ts - connect_trial["time_elapsed"]
    print(f"[CLOCK] Estimated jsPsych start offset: {offset:.1f} ms")
    print(f"        (first gaze local_ts={first_gaze_ts:.1f}, "
          f"connect trial time_elapsed={connect_trial['time_elapsed']})\n")
    return offset


def epoch_gaze(gaze, trials, offset):
    """Extract gaze samples for each reading trial using time alignment."""
    reading_trials = [t for t in trials if t.get("task") == "reading"]
    epochs = []

    for trial in reading_trials:
        # Sentence was on screen from (time_elapsed - rt) to time_elapsed
        onset_te = trial["time_elapsed"] - trial["rt_ms"]
        offset_te = trial["time_elapsed"]

        # Convert to local_ts
        onset_lt = onset_te + offset
        offset_lt = offset_te + offset

        # Filter gaze samples
        samples = [g for g in gaze if onset_lt <= g["local_ts"] <= offset_lt]

        epochs.append({
            "sentence": trial["sentence"],
            "rt_ms": trial["rt_ms"],
            "trial_index": trial["trial_index"],
            "samples": samples,
            "onset_lt": onset_lt,
            "offset_lt": offset_lt,
        })

    return epochs


def extract_fixations(samples):
    """Group samples by fpogid into fixations."""
    if not samples:
        return []

    fixations = []
    current_id = None
    current_group = []

    for s in samples:
        fid = s["fpogid"]
        if fid != current_id:
            if current_group:
                fixations.append(summarise_fixation(current_group))
            current_id = fid
            current_group = [s]
        else:
            current_group.append(s)

    if current_group:
        fixations.append(summarise_fixation(current_group))

    return fixations


def summarise_fixation(group):
    valid = [s for s in group if s["fpogv"] == 1]
    if not valid:
        valid = group  # fallback

    xs = [s["fpogx"] for s in valid]
    ys = [s["fpogy"] for s in valid]

    return {
        "fixation_id": group[0]["fpogid"],
        "x_norm": np.mean(xs),
        "y_norm": np.mean(ys),
        "x_px": np.mean(xs) * SCREEN_W,
        "y_px": np.mean(ys) * SCREEN_H,
        "duration_s": max(s["fpogd"] for s in group),
        "n_samples": len(group),
        "n_valid": len([s for s in group if s["fpogv"] == 1]),
        "time_start": group[0]["local_ts"],
        "time_end": group[-1]["local_ts"],
    }


def map_fixation_to_word(fix_x_px, sentence):
    """Map fixation x-pixel to word index (centered sentence)."""
    sentence_width = len(sentence) * CHAR_WIDTH_PX
    x_offset = (SCREEN_W - sentence_width) / 2
    char_pos = (fix_x_px - x_offset) / CHAR_WIDTH_PX

    if char_pos < 0 or char_pos > len(sentence):
        return None, "?"

    words = sentence.split()
    cumulative = 0
    for wi, word in enumerate(words):
        word_start = cumulative
        word_end = cumulative + len(word)
        if word_start - 0.5 <= char_pos <= word_end + 0.5:
            return wi, word
        cumulative = word_end + 1
    return None, "?"


def classify_saccades(fixations):
    """Label each saccade as forward, regression, or line_change."""
    for i in range(len(fixations)):
        if i == 0:
            fixations[i]["saccade"] = "first"
        else:
            dx = fixations[i]["x_px"] - fixations[i-1]["x_px"]
            dy = abs(fixations[i]["y_px"] - fixations[i-1]["y_px"])
            if dy > FONT_SIZE_PX:
                fixations[i]["saccade"] = "line_change"
            elif dx > 0:
                fixations[i]["saccade"] = "forward"
            else:
                fixations[i]["saccade"] = "regression"
    return fixations


def analyse(path, screen_w=1920, screen_h=1080):
    global SCREEN_W, SCREEN_H
    SCREEN_W, SCREEN_H = screen_w, screen_h

    gaze, trials = load_results(path)
    print(f"Loaded {len(gaze)} gaze samples, {len(trials)} jsPsych trials")
    print(f"Screen: {SCREEN_W}×{SCREEN_H}\n")

    offset = estimate_clock_offset(gaze, trials)
    epochs = epoch_gaze(gaze, trials, offset)

    for ep in epochs:
        sentence = ep["sentence"]
        words = sentence.split()
        print(f"\n{'='*70}")
        print(f"Sentence: {sentence}")
        print(f"Reading time: {ep['rt_ms']} ms | Samples in epoch: {len(ep['samples'])}")
        print(f"{'='*70}")

        if not ep["samples"]:
            print("  ⚠ No gaze samples in this epoch!")
            print(f"    Expected local_ts range: {ep['onset_lt']:.0f} – {ep['offset_lt']:.0f}")
            all_ts = [g["local_ts"] for g in gaze]
            print(f"    Gaze local_ts range: {min(all_ts):.0f} – {max(all_ts):.0f}")
            continue

        fixations = extract_fixations(ep["samples"])
        # Keep only fixations with valid data
        fixations = [f for f in fixations if f["n_valid"] > 0]
        fixations = classify_saccades(fixations)

        print(f"\n  {'#':<5} {'Word':<15} {'Dur(ms)':<10} {'Saccade':<12} "
              f"{'X(px)':>7} {'Y(px)':>7} {'Valid':>5}")
        print(f"  {'-'*65}")

        for f in fixations:
            wi, word = map_fixation_to_word(f["x_px"], sentence)
            f["word_index"] = wi
            f["word"] = word
            print(f"  {f['fixation_id']:<5} {word:<15} "
                  f"{f['duration_s']*1000:>7.0f}   {f['saccade']:<12} "
                  f"{f['x_px']:>7.0f} {f['y_px']:>7.0f} "
                  f"{f['n_valid']:>5}/{f['n_samples']}")

        n_fwd = sum(1 for f in fixations if f["saccade"] == "forward")
        n_reg = sum(1 for f in fixations if f["saccade"] == "regression")
        durations = [f["duration_s"]*1000 for f in fixations]

        print(f"\n  Summary:")
        print(f"    Total fixations:   {len(fixations)}")
        print(f"    Forward saccades:  {n_fwd}")
        print(f"    Regressions:       {n_reg}")
        if n_fwd + n_reg > 0:
            print(f"    Regression rate:   {n_reg/(n_fwd+n_reg)*100:.1f}%")
        if durations:
            print(f"    Mean fixation:     {np.mean(durations):.0f} ms")
            print(f"    Median fixation:   {np.median(durations):.0f} ms")

        # Per-word summary
        print(f"\n  Per-word fixation time:")
        for wi, word in enumerate(words):
            word_fixes = [f for f in fixations if f["word_index"] == wi]
            total_ms = sum(f["duration_s"]*1000 for f in word_fixes)
            count = len(word_fixes)
            if count > 0:
                first_ms = word_fixes[0]["duration_s"]*1000
                print(f"    {wi:>2}. {word:<15} "
                      f"visits={count}  first_fix={first_ms:.0f}ms  "
                      f"total={total_ms:.0f}ms")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python analyse_gaze.py <results.json> [screen_w] [screen_h]")
        sys.exit(1)

    path = sys.argv[1]
    sw = int(sys.argv[2]) if len(sys.argv) > 2 else 1920
    sh = int(sys.argv[3]) if len(sys.argv) > 3 else 1080
    analyse(path, sw, sh)
