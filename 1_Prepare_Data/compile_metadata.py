import os, time, sys
from redis import Redis
import codecs

r = Redis()

output = "output"
outputfile = "metadatalist.txt"

cur = 0.0
st = time.time()

def get_row():
  if r.llen(output) != 0:
    row = r.rpop(output).decode("utf-8")
    return row
  else:
    return False

if __name__ == "__main__":
  end = False
  compiledfile = codecs.open(outputfile, "w", encoding="utf-8")
  while(True):
    row = get_row()
    while(row):
      try:
        if row == "1":
          row = False
          end = True
        else:
          compiledfile.write(u"{0}\n".format(row))
          row = get_row()
      except OSError, e:
        row = get_row()        
    if not end:
      print("Compiler ran out of jobs - waiting for 10 seconds before checking again")
      time.sleep(10)
    else:
      compiledfile.close()
      sys.exit(0)
