# GazePoint Reading Experiment (jsPsych + JATOS)

Eye-tracking reading experiment that measures fixation durations, saccades, and regressions as participants read sentences displayed in large monospaced font.

## Architecture

```
GazePoint GP3 hardware
    ↓ USB
Gazepoint Control (desktop app — handles calibration)
    ↓ TCP 127.0.0.1:4242 (Open Gaze API, XML)
gp_relay.py (Python WebSocket bridge)
    ↓ ws://localhost:8765 (JSON)
experiment.html (jsPsych in browser / JATOS)
```

## Files

| File | Purpose |
|---|---|
| `gp_relay.py` | WebSocket relay — bridges GazePoint TCP → browser WebSocket |
| `experiment.html` | jsPsych 7 experiment: connect → calibrate → read sentences |
| `analyse_gaze.py` | Post-hoc analysis: fixations, saccades, regressions per word |

## Setup

### 1. Install Python dependency

```bash
pip install websockets
```

### 2. Run the experiment

1. Plug in the GazePoint tracker and open **Gazepoint Control**.
2. Start the relay:
   ```bash
   python gp_relay.py
   ```
3. Open `experiment.html` in a browser (or launch via JATOS).

### 3. Import into JATOS

1. In JATOS, create a new study.
2. Create a component and upload `experiment.html` as the HTML file.
3. The participant's machine still needs to run `gp_relay.py` locally.

## Configuration

In `experiment.html`:

- `SENTENCES` — edit the array to change stimuli
- `SHOW_GAZE_DOT` — set `false` for production (hides the red tracking dot)
- Font size is 64px Courier New with 4px letter-spacing → each character ≈ 38px wide

In `analyse_gaze.py`:

- `SCREEN_W`, `SCREEN_H` — match participant's resolution
- `FONT_SIZE_PX`, `CHAR_WIDTH_PX` — match the CSS in experiment.html

## Data format

The experiment saves a JSON object with two fields:

- `jspsych`: standard jsPsych trial data (RT, sentence, trial index)
- `gaze`: array of gaze samples, each containing:
  - `fpogx`, `fpogy` — fixation point-of-gaze (normalised 0–1)
  - `fpogd` — fixation duration (seconds)
  - `fpogid` — fixation ID (increments on each new fixation)
  - `fpogv` — fixation valid flag (1 = valid)
  - `user_data` — trigger label (e.g. `sentence_onset_sentence_0`)
  - `time` — tracker timestamp

## Analysis

```bash
python analyse_gaze.py results.json
```

Outputs per sentence: fixation table with word mapping, forward/regression saccade counts, regression rate, and mean fixation duration.

## Timing precision

- WebSocket round-trip on localhost: ~1–2 ms
- Triggers use GazePoint's `USER_DATA` field → marker is embedded in the tracker's own data stream with tracker-clock timestamps
- No post-hoc clock alignment needed

## Calibration tips

- The large font (64px) is forgiving of ±1° calibration error
- Use the gaze-check screen (red dot) to verify before starting
- Re-calibrate if the dot consistently misses by >1 cm
