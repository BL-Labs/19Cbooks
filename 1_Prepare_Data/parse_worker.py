import time
import os, sys
from redis import Redis

from parse_xml import parse_xml

r = Redis()

wq = "q"
output = "output"

def get_job(workerid):
  if r.llen(workerid) == 0:
    status = r.rpoplpush(wq, workerid)
    return status
  else:
    return r.lrange(workerid, 0, 0)[0]

def clear_job(workerid, job):
  r.lrem(workerid, job, 1)

if __name__ == "__main__":
  workerid = "wrk"+sys.argv[1]
  while(True):
    job = get_job(workerid)
    while(job):
      try:
        row = parse_xml(job)
        r.lpush(output, u"\t".join(row).encode("utf-8"))
        clear_job(workerid, job)
      except OSError, e:
        clear_job(workerid, job)
      job = get_job(workerid)
    print("%s ran out of jobs - waiting for 10 seconds before checking again" % workerid)
    time.sleep(10)
