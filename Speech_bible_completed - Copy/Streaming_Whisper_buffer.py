"""
Streaming_whisper_buffer.py

Rolling microphone buffer + Whisper transcription.

The transcription can be sent directly to Main.py through
the on_transcript callback.

output.txt is still written as a debug/log file.

This version adds runtime-configurable settings (sample rate,
buffer duration, processing interval, whisper model) plus
start/stop control and response-time tracking, so the dashboard
can drive this module through Main.py's Flask routes.
"""

import collections
import time
import threading
import os
import json

import numpy as np
import sounddevice as sd
from faster_whisper import WhisperModel
from num2words import num2words

# -------------------------------- CONFIG ------------------------------------
# These are now mutable at runtime via apply_settings(). They only change
# while the system is stopped (is_running == False).

SAMPLE_RATE = 16000
CHANNELS = 1

CHUNK_DURATION = 0.8
BUFFER_DURATION = 10
TRANSCRIBE_INTERVAL = 4.0

MODEL_SIZE = "base"

OUTPUT_PATH = os.path.join(
    os.path.dirname(__file__),
    "output.txt"
)

LANG = "en"
BEAM_SIZE = 2

BLOCK_SIZE = int(SAMPLE_RATE * CHUNK_DURATION)

# -----------------------------------------------------------------------------

# Make sure output directory exists
output_dir = os.path.dirname(OUTPUT_PATH)

if output_dir:
    os.makedirs(output_dir, exist_ok=True)

# ------------------------------ LOAD MODEL ----------------------------------

print(f"Loading model '{MODEL_SIZE}' (this may take a while)...")

model = WhisperModel(
    MODEL_SIZE,
    device="cpu",
    compute_type="int8"
)

print("Model loaded.")

# ------------------------------ AUDIO BUFFER ---------------------------------

max_samples = int(BUFFER_DURATION * SAMPLE_RATE)

audio_buffer = collections.deque(maxlen=max_samples)

# ------------------------------ BIBLE DATA -----------------------------------

with open(
    os.path.join(os.path.dirname(__file__), "data", "kjv.json"),
    "r",
    encoding="utf-8"
) as f:

    bible = json.load(f)["verses"]

    names = list({
        name["book_name"]
        for name in bible
    })

focus_words = names + [
    "chapter",
    "verse",
    "in the book of",
    "scripture",
    "first",
    "second",
    "third"
]

num_words = (
    [num2words(i) for i in range(0, 11)]
    + [num2words(i) for i in range(0, 110, 10)]
    + [str(i) for i in range(1, 180)]
)

prompt_words = " ".join(focus_words)

# ------------------------------ THREAD CONTROL ------------------------------

stop_event = threading.Event()

# Main.py will assign its detect_scripture function here.
#
# Example:
#
# streaming.on_transcript = detect_scripture
#
on_transcript = None

# Current microphone volume for the dashboard
current_volume = 0.0

# Latest transcription of the rolling audio buffer
full_text = None

# Whether the audio stream + transcription loop are currently active
is_running = False

# Time (seconds) it took the last NEW transcript to go from
# "start of transcription" to "verse detection callback finished".
# This is the dashboard's "speech to delivered text" metric.
last_response_time = 0.0

# ------------------------------ SETTINGS API ---------------------------------

# Allowed option sets. Defined here so both the backend and the dashboard
# agree on what values are valid. The dashboard's dropdowns should mirror
# these.
ALLOWED_SAMPLE_RATES = (8000, 16000, 32000, 48000)
ALLOWED_BUFFER_DURATIONS = (5, 10, 20, 30)
ALLOWED_PROCESSING_INTERVALS = (2, 4, 8, 15)
ALLOWED_MODELS = ("tiny", "base", "small", "medium")


def get_settings():
    """Return the currently active settings as plain JSON-friendly types."""
    return {
        "sample_rate": SAMPLE_RATE,
        "buffer_duration": BUFFER_DURATION,
        "processing_interval": TRANSCRIBE_INTERVAL,
        "model": MODEL_SIZE,
    }


def apply_settings(sample_rate=None, buffer_duration=None,
                    processing_interval=None, model_size=None):
    """
    Apply new settings. Only allowed while the system is stopped, since
    changing sample rate / buffer size / model requires rebuilding the
    audio stream, buffer, and (possibly) the Whisper model itself.
    """
    global SAMPLE_RATE, BUFFER_DURATION, TRANSCRIBE_INTERVAL, BLOCK_SIZE
    global MODEL_SIZE, model, audio_buffer, max_samples

    if is_running:
        raise RuntimeError(
            "Cannot change settings while the system is running. "
            "Press STOP first."
        )

    if sample_rate is not None:
        sample_rate = int(sample_rate)
        if sample_rate not in ALLOWED_SAMPLE_RATES:
            raise ValueError(f"Invalid sample_rate: {sample_rate}")
        SAMPLE_RATE = sample_rate

    if buffer_duration is not None:
        buffer_duration = float(buffer_duration)
        if int(buffer_duration) not in ALLOWED_BUFFER_DURATIONS:
            raise ValueError(f"Invalid buffer_duration: {buffer_duration}")
        BUFFER_DURATION = buffer_duration

    if processing_interval is not None:
        processing_interval = float(processing_interval)
        if int(processing_interval) not in ALLOWED_PROCESSING_INTERVALS:
            raise ValueError(f"Invalid processing_interval: {processing_interval}")
        TRANSCRIBE_INTERVAL = processing_interval

    if model_size is not None and model_size != MODEL_SIZE:
        if model_size not in ALLOWED_MODELS:
            raise ValueError(f"Invalid model: {model_size}")
        print(f"Loading Whisper model '{model_size}'...")
        model = WhisperModel(model_size, device="cpu", compute_type="int8")
        MODEL_SIZE = model_size
        print("Model loaded.")

    # Rebuild derived values / buffer for the (possibly new) sample rate
    # and buffer duration.
    BLOCK_SIZE = int(SAMPLE_RATE * CHUNK_DURATION)
    max_samples = int(BUFFER_DURATION * SAMPLE_RATE)
    audio_buffer = collections.deque(maxlen=max_samples)


def reset_to_defaults():
    """Restore the documented default settings."""
    apply_settings(
        sample_rate=16000,
        buffer_duration=10,
        processing_interval=4.0,
        model_size="base",
    )

# ------------------------------ AUDIO FUNCTIONS ------------------------------

def append_to_buffer(chunk: np.ndarray):
    """
    Append a mono float32 audio chunk to the rolling buffer.
    """

    audio_buffer.extend(chunk.tolist())

def sd_callback(indata, frames, time_info, status):
    global current_volume

    if status:
        pass

    # Convert input to mono
    if indata.ndim == 1:
        arr = indata

    elif CHANNELS == 1:
        arr = indata[:, 0]

    else:
        arr = indata.mean(axis=1)

    # Ensure float32
    if arr.dtype != np.float32:
        arr = arr.astype(np.float32)

    # ---------------- VOLUME CALCULATION ----------------

    rms = np.sqrt(np.mean(arr ** 2))

    # Convert RMS to dB
    db = 20 * np.log10(max(rms, 1e-7))

    # Map approximately -60 dB → 0%
    # and 0 dB → 100%
    volume = ((db + 60) / 60) * 100

    # Keep between 0 and 100
    current_volume = float(max(0.0, min(100.0, volume)))

    # -----------------------------------------------------

    # Add audio to Whisper buffer
    append_to_buffer(arr)

# ------------------------------ WHISPER --------------------------------------

def transcribe_current_buffer(i):
    """
    Transcribe the current rolling audio buffer.

    Returns:
        str  -> transcription
        None -> insufficient audio
    """

    print("transcribe_current_buffer running ..")

    # Wait until we have at least 2 seconds of audio
    if len(audio_buffer) < SAMPLE_RATE * 2:
        return None

    # Copy buffer into numpy array
    audio_np = np.array(
        audio_buffer,
        dtype=np.float32
    )

    start_time = time.time()

    segments, info = model.transcribe(
        audio_np,
        beam_size=BEAM_SIZE,
        language=LANG,
        vad_filter=True,
        initial_prompt=prompt_words
    )

    print("transcribed")

    # Collect segment text
    text = " ".join(
        seg.text.strip()
        for seg in segments
    ).strip()

    transcription_time = time.time() - start_time

    print(
        f"transcription time: "
        f"{transcription_time:.2f}s"
    )

    return text

# ------------------------------ OUTPUT FILE ----------------------------------

def write_transcript_files(text: str):

    ts = time.strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    entry = f"[{ts}], {text}\n"

    with open(
        OUTPUT_PATH,
        "w",
        encoding="utf-8"
    ) as f:

        f.write(entry)
        f.flush()

    print("write_transcript_files ran.")

# ------------------------------ TRANSCRIPTION LOOP ---------------------------

def transcribe_loop():

    print("Transcription loop started.")

    global full_text, last_response_time

    last_full_text = ""

    i = 1

    while not stop_event.is_set():

        # cycle_start marks the beginning of "speech -> delivered text"
        # for whatever new transcript comes out of this cycle.
        cycle_start = time.time()

        full_text = transcribe_current_buffer(i)

        i += 1

        print(
            f"transcribed text: {full_text}"
        )

        if full_text:

            print(
                f"-> {full_text}"
            )

            # Only send new/different transcription
            if full_text != last_full_text:

                # Keep writing output.txt
                write_transcript_files(
                    full_text
                )

                # Send transcription directly to Main.py
                if on_transcript:

                    try:

                        on_transcript(
                            full_text
                        )

                    except Exception as e:

                        print(
                            "on_transcript callback error:",
                            e
                        )

                # Total time from starting this transcription cycle to the
                # verse-detection callback finishing == "speech to
                # delivered text" for the dashboard.
                last_response_time = time.time() - cycle_start

                last_full_text = full_text

        else:

            print("silence..")

        # Maintain processing interval
        elapsed = time.time() - cycle_start

        to_sleep = max(
            0.0,
            TRANSCRIBE_INTERVAL - elapsed
        )

        step = 0.1

        slept = 0.0

        while (
            slept < to_sleep
            and not stop_event.is_set()
        ):

            sleep_time = min(
                step,
                to_sleep - slept
            )

            time.sleep(sleep_time)

            slept += sleep_time

        print(
            f"processing took {elapsed:.2f}s"
        )

        print(
            f"slept {to_sleep:.2f}s"
        )

    print(
        "Transcription loop stopped."
    )

# ------------------------------ START / STOP STREAMING ------------------------

def start_streaming():
    """
    Blocking call: opens the microphone stream and runs the transcription
    loop until stop_streaming() is called (or the process is interrupted).
    Main.py should run this inside its own thread so the Flask server
    stays responsive.
    """

    global is_running

    # Make sure the event isn't already set
    stop_event.clear()
    is_running = True

    # Start Whisper transcription thread
    t = threading.Thread(
        target=transcribe_loop,
        daemon=True
    )

    t.start()

    try:

        with sd.InputStream(
            samplerate=SAMPLE_RATE,
            channels=CHANNELS,
            dtype="float32",
            blocksize=BLOCK_SIZE,
            callback=sd_callback
        ):

            print(
                "Listening..."
            )

            while not stop_event.is_set():

                time.sleep(0.5)

    except KeyboardInterrupt:

        print(
            "KeyboardInterrupt received: stopping..."
        )

    finally:

        stop_event.set()

        t.join()

        is_running = False

        print(
            "Streaming stopped cleanly."
        )


def stop_streaming():
    """
    Signal the running audio stream / transcription loop to stop.
    Non-blocking: start_streaming()'s own thread will unwind and flip
    is_running back to False once it notices stop_event.
    """
    stop_event.set()

# ------------------------------ DIRECT RUN -----------------------------------

if __name__ == "__main__":

    start_streaming()