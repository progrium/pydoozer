#!/usr/bin/python
import os
import sys
sys.path.append(os.path.dirname(__file__) + "/..")

import gevent
import doozer
import simplejson

from gevent import Timeout

client = doozer.connect()

#clean out the foo dir.
walk = client.walk("/foo/**")
for node in walk:
    client.delete(node.path, node.rev)

rev = client.set("/foo/bar", "test", 0).rev

def watch_test(rev):
    while True:
        try:
            change = client.wait("/foo/**", rev )
            print "saw change at %s with %s" % ( change.rev, change.value)
            rev = change.rev+1
        except Timeout, t:
            change = None
            print t
            rev = client.rev().rev
            #rev =+1

#spawn the process that watches the foo dir for changes.
watch_job = gevent.spawn(watch_test, rev+1)

#add new data in foo
for i in range(10):
    gevent.sleep(0.5)
    revk = client.set("/foo/bar%d" % i, simplejson.dumps({'data': i}), 0).rev

foo = client.getdir("/foo")

print "Directly under /foo is "
for f in foo:
    print f.path, f.rev,
    print client.get("/foo/"+f.path).value

    dir(f)

client.disconnect()
watch_job.kill()

