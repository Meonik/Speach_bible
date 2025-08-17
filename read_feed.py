import time
import soundfile as sf
import numpy as np
import resampy as rs

SAMPLE_AUDIO_PATH = r"data\10th-august-225_EOPwctfY.mp3"
def load_audio_frames_channels(path, sr=16000):
  start = time.time()
  samples, sample_rate = sf.read(path, dtype = "float32")
  read_time = time.time() - start
  samples = np.squeeze(samples)
  squeeze_time = time.time() - read_time
  if np.ndim(samples)>1:
    samples = samples.mean(axis=1)
    averaging_time = time.time() - squeeze_time
  if sample_rate != sr:
    samples = rs.resample(samples, sample_rate, sr)
    resampling_time = time.time() - averaging_time
  else:
    print("mono already")
  return samples, sr, read_time, squeeze_time, averaging_time, resampling_time

samples, sr, read_time, squeeze_time, averaging_time, resampling_time = load_audio_frames_channels(SAMPLE_AUDIO_PATH)
with open('data/audio_array.txt', "w", encoding="utf-8") as N:
  N.write("Audio data export\n")
  N.write(f"Sample rate: {sr} Hz\n")
  N.write(f"channels: 1\n shape: {np.shape(samples)}\n")
  N.write(f"read time: {read_time}\nsqueeze_time: {squeeze_time}\naveraging_time: {averaging_time}\nresampling_time: {resampling_time}\n\n")

np.save("data/10th_aug_audio_data.npy", samples)

print(sr, samples, np.shape(samples))

# print(sf.info(SAMPLE_AUDIO_PATH))