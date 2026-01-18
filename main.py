"""re -> short for regex(i.e regular expression) used for finding matching, and manipulating text based on patterens rather than exact words.
Normal string search is limited- you can only look for exact matches. regex can look for shapes of text.
Daemon threads:
1. runs in the background
2. automatically stops when the main program stops.
"""


import re, json, time, threading, os
from flask import Flask, Response, send_file
if __name__  == "__main__":
  from Streaming_Whisper_buffer import start_streaming

app = Flask(__name__)

current_verse = "Waiting for verse ..."

#Load Bible text
with open(r"data/kjv.json", "r") as f:
  bible = json.load(f)["verses"]  # returns a list of dictionaries containing the info of a particular verse.

#print(type(bible))
# Book list for regex
book_names = sorted(set([v["book_name"] for v in bible]), key=len, reverse=True) 
# sorts with the highest length of string of the book names first because regex matches longer book names first to prevent partial matches.
pattern = re.compile(rf"({'|'.join(book_names)})\s+(\d{{1,3}}):(\d{{1,3}})", re.IGNORECASE)
r""" we need to compile/parse it once cause it faster cause the computer don't have to try to understand the structure of the string everytime we try to use the pattern as it would do if was just an ordinary string.
 the parttern contains all the bible names seperated with the '|' character as it means OR to regex, and one spaces or more and numbers of 1-3 digits a literal ':' and then another number of 1-3 digits, the second option is to ignore cases
 \s  -> single space
 + -> one or more of any thing it in front of.
 \d -> number (0-9)
"""

def check_num(x):
  da = x.split()
  if da[0].isnumeric():
    return x[2:]
  return x

just_book_names = list(map(check_num ,book_names))
pattern2 = re.compile(rf"(first|second|third)?\s*({'|'.join(just_book_names)})\s+(\d{{1,3}})[A-Za-z\s]*\s+(\d{{1,3}})", re.IGNORECASE)

def search_bible(book, chapter, verse):
  print("searched_bible")
  found = [v for v in bible if book == v["book_name"] and chapter == v["chapter"] and verse == v["verse"]]
  if len(found) > 1 or len(found) == 0:
    return f"found {len(found)} matches"
  else:
    return found[0].get("text", "Verse not found.")

@app.route("/") # from flask it runs the fuction underneath if the user enters a URL of '/' in the browser.
def index():
  print("index ran")
  return send_file("overlay.html")

@app.route("/verse-stream")
def verse_stream():
  def stream():
    last_verse = ""
    while True:
      print("stream ...")
      global current_verse # uses the current_verse variable from the global scope, not a local copy. its somewhere else in the code being updated
      if current_verse != last_verse:
        yield f"data: {current_verse}\n\n"  #sends verse to the browser in SSE format (server-sent-event) making this a generator function.
        last_verse = current_verse
      time.sleep(1) # to avoid flooding the client.
  return Response(stream(),mimetype='text/event-stream')  # wraps the generator function into a flask response object and mimetype tells the browser this is an SSE data not HTML or JSON
# The browser will keep the connection open and keep reading new mesages as they come

def detect_scripture(transcribed_text):
  global current_verse
  match = pattern.search(transcribed_text)
  match2 = pattern2.search(transcribed_text)
  print("detect_scripture ran")
  if match:
    book, chapter, verse = match.groups()
    book = book.title()   # converts to title case
    chapter, verse = int(chapter), int(verse)
    verse_text = search_bible(book, chapter, verse)
    current_verse = f"{book} {chapter}:{verse} - {verse_text}"
    print(current_verse)
  elif match2:
    position, book, chapter, verse = match2.groups()
    print(position, book, chapter, verse)

    if not position:
      p = ''
    else:
      position = position.lower()
      if position == "first":
        p = 1
      elif position == "second":
        p = 2
      elif position == "third":
        p = 3
        
    book = (f"{p} " + book).strip().title()   # converts to title case
    chapter, verse = int(chapter), int(verse)
    verse_text = search_bible(book, chapter, verse)
    current_verse = f"{book} {chapter}:{verse} - {verse_text}"
    print(current_verse)
  else:
    current_verse = f"{transcribed_text} - {type(transcribed_text)} No verse detected."


def watch_transcript():
  print("watch_transcript running..")
  last_content = ""
  while True:
    if os.path.exists("output.txt"):  #checks if the path to the file exists in the folder this file is in.
      with open("output.txt") as f:
        transcript = f.read().strip()
        if transcript != last_content:
          detect_scripture(transcript)
          last_content = transcript
    time.sleep(1)


if __name__ == "__main__": # prevent the server from auto_starting if this file is imported as a module. 
  t = threading.Thread(target=watch_transcript, daemon=True)
  t.start()  # calls the watch_transcript in another daemon thread
  t2 = threading.Thread(target=start_streaming, daemon=True)
  t2.start()
  app.run(host="127.0.0.1", port=5000)

# t.join()
# print("thread stopped")