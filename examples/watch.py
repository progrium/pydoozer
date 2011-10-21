import os
import sys
sys.path.append(os.path.dirname(__file__) + "/..")

import gevent
import doozer

from gevent import Timeout

client = doozer.connect()
rev = client.rev().rev

def watch_test(rev):
    try:
        hmmz = client.wait("/watch", rev+1)
    except Timeout, t:
        print t
        watch_test(rev)
        return
    print hmmz.rev, hmmz.value
    watch_test(hmmz.rev+1)

watch_job = gevent.spawn(watch_test, rev)

for i in range(10):
    gevent.sleep(1)
    rev = client.set("/watch", "test4%d" % i, rev).rev
    print rev


foo = client.get("/watch")
print "Got /watch with %s" % foo.value

gevent.sleep(2)
client.delete("/watch", rev)
print "Deleted /watch"

foo = client.get("/watch")
print foo

client.disconnect()
watch_job.kill()
