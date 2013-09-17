import os, time
from redis import Redis

r = Redis()

root = "md"

q = "q"
LIMIT = 3000
PAUSE = 15

cur = 0.0
st = time.time()

for div in os.listdir(root):
  for ident in os.listdir(os.path.join(root, div)):
    cur += 1
    r.lpush(q, ident)
    if not cur % 100:
      print("{0} added. {1}/s".format(cur, (time.time()-st) / float(cur) ))
      if r.llen(q) > LIMIT:
        print("Input Queue over {0}, pausing for {1}s".format(LIMIT, PAUSE))
        time.sleep(PAUSE)
