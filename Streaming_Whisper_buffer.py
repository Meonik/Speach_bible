"""
Streaming_whisper_buffer.py

- Rolling 302 audio buffer from microphone
- Every TRANSCRIBE_INTERVAL seconds send latest 30s to whisper
- writes transcription to data/output.txt and optionally calls a callback
"""
import collections, time, threading, os
import numpy as np
import sounddevice as sd
from faster_whisper import WhisperModel

# --------------------------------CONFIG ------------------------------------
SAMPLE_RATE = 16000                                 # Whisper prefers 16000
CHANNELS = 1
CHUNK_DURATION = 0.8                               # seconds append to buffer each callback
BUFFER_DURATION = 30                              # seconds in rolling buffer (Whisper context)
TRANSCRIBE_INTERVAL = 4.0                           # seconds between transcription runs
MODEL_SIZE = "tiny"                               
OUTPUT_PATH = os.path.join("data","output.txt")
LANG = "en"                                          # language for transcription
BEAM_SIZE = 1                                      # beam size for transcription (higher= potentially more accurate, slower)
BLOCK_SIZE = int(SAMPLE_RATE * CHUNK_DURATION)
SAMPLE_AUDIO_PATH = r"data\10th-august-225_EOPwctfY.mp3"
# -------------------------------------------------------------------------------------

#Make sure data folder exists
os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

# load Whisper model (fast-whisper)
print(f"Loading model '{MODEL_SIZE}' (this may take a while)...")
model = WhisperModel(MODEL_SIZE, device="cpu", compute_type="int8") # change device/compute_type if you have GPU
print("Model loaded.")

# Rolling buffer (deque of floats)
max_samples = int(BUFFER_DURATION*SAMPLE_RATE)
audio_buffer = collections.deque(maxlen=max_samples)

# Thread control
stop_event = threading.Event()

# Optional callback: set this to your detect function if you want to call it directly.
# Example: from main import detect_scripture
# then set on_transcript = detect_scripture
on_transcript = None

# Helper to append numpy audio (float32, mono) to buffer
def append_to_buffer(chunk: np.ndarray):
  """
  chunk: 1-D numpy float32 array (monophonic)
  """
  # print("append_to_buffer ran.")
  audio_buffer.extend(chunk.tolist())

# SD callback receives small chunks: convert to float32 and append
def sd_callback(indata, frames, time_info, status):
  # indata shape: (frames, channels)
  # print("sd_callback ran.")
  if status:
    # You might want to log status messsages in production
    pass
  if indata.ndim == 1:
    arr = indata
  elif CHANNELS == 1:
    arr = indata[:,0]
  else:
    arr = indata.mean(axis= 1) # mixdown
  # Ensure float32l
  if arr.dtype != np.float32:
    arr = arr.astype(np.float32)
  append_to_buffer(arr)

def transcribe_current_buffer():
  """
  Convert the rolling buffer to numpy array and transcribe whole buffer wit Whisper.
  Returns the full transcription txt (String) or None if not enough audio.
  """
  print("transcribe_current_buffer running ..")
  if len(audio_buffer) < SAMPLE_RATE * 5: # waits until at least 3s captured
    return None
  
  # Convert deque to Numpy array
  audio_np = np.array(audio_buffer, dtype=np.float32)
  # print(audio_np)
  # faster-whisper accepts numpy audio as input (1-D float32)
  # We request segments to collect text: using beam_size for accuracy.
  time_s = time.time()
  segments, info = model.transcribe(
    audio_np, 
    beam_size=BEAM_SIZE, 
    language=LANG,
  )
  print("transcribed")
  # print("transcribe_current_buffer ran.")
  # Join segment txts (keep them in order)
  text = " ".join([seg.text.strip() for seg in segments]).strip()
  print(f"transcription time: {time.time()- time_s}")
  print("joint")
  return text

def write_transcript_files(text: str):
  # Append or overwrite? We will append a timestamped entry for review.
  ts = time.strftime("%Y-%m-%d %H:%M:%S")
  entry = f"[{ts}], {text}\n"
  with open(OUTPUT_PATH, "a", encoding="utf-8") as f:
    f.write(entry)
    f.flush()
    print("write_transcript_files ran.")

# Main loop that runs in a thread and transcribes every TRANSCRIBE_INTERVAL seconds
def transcribe_loop():
  # print("Transcription loop started.")
  last_full_text = "" # hold last full transcriptiont to detect deltas if you wish

  while not stop_event.is_set():
    start_t = time.time()
    print("time started.")
    full_text = transcribe_current_buffer()
    print(f"transcribed text: {full_text}")
    if full_text:
      print(f"-> {full_text}")
      # Option:dedup repeated results by comparing with last
      if full_text != last_full_text:
        # Optionally compute new_text = difference between last_full_text and full_text
        # For now we just write the full_text
        write_transcript_files(full_text)

        if on_transcript:
          try:
            # if Your detect_scripture expects only the new portion, you can compute delta here.
            on_transcript(full_text)
          except Exception as e:
            print("on_transcript callback error:", e)

        last_full_text = full_text
      
      else:
        # not enough audio yet
        pass
      
      # Sleep until next interval, but check stop_event periodically
      elapsed =  time.time() - start_t
      to_sleep = max(0.0, TRANSCRIBE_INTERVAL - elapsed)
      # Sleep in small steps to be responsive to stop_event
      step = 0.1
      slept = 0.0
      while slept < to_sleep and not stop_event.is_set():
        time.sleep(min(step, to_sleep - slept))
        slept += step
      
      print(elapsed)
      print(f"slept {to_sleep}s")

    else: print("silence..")

  print("Transcription loop stopped.")


#Entrypoint - start audio strean and trascription thread
def start_streaming():
  # Start transcripton thread
  t = threading.Thread(target=transcribe_loop, daemon=True)
  t.start()

  # Open sounddevice input stream (blocks) until stop_event
  try:
    # with sd.InputStream(samplerate=SAMPLE_RATE, channels=CHANNELS,
    #                     dtype="float32",
    #                     blocksize=BLOCK_SIZE,
    #                     callback=sd_callback):
    
      print("Listening (ress Ctrl+c to stop)...")

      samples = np.load(r"data\10th_aug_audio_data.npy", mmap_mode="r")
      start = 0
      
      total_samples = len(samples)
      while not stop_event.is_set() and  start < total_samples:
        time_started = time.time()
        end = min(BLOCK_SIZE+start, total_samples)
        chunk = samples[start:end]
        sd_callback(chunk, len(chunk), None, None)
        start += BLOCK_SIZE
        time.sleep(CHUNK_DURATION - (time.time()-time_started))

      while not stop_event.is_set():
        time.sleep(0.5)
  except KeyboardInterrupt:
    print("KeybaordInterrupt receivd: stopping...")
  finally:
    stop_event.set()
    t.join()
    print("Streaming stopped cleanly.")


# If run directly, start streaming
# if __name__ == "__main__":
  # Example: if you want to call main.detect_scripture directly, import it here and set no_transcript:
  # from main import detect_scripture
  # on_transcript = detect_scripture

start_streaming()

  