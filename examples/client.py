import os
import sys
sys.path.append(os.path.dirname(__file__) + "/..")

import doozer

client = doozer.connect()

rev = client.set("/foo", "test", 0).rev
print "Setting /foo to test with rev %s" % rev

foo = client.get("/foo")
print "Got /foo with %s" % foo.value

root = client.getdir("/")
print "Directly under / is %s" % ', '.join([file.path for file in root])

client.delete("/foo", rev)
print "Deleted /foo"

foo = client.get("/foo")
print repr(foo)

walk = client.walk("/**")
for file in walk:
    print ' '.join([file.path, str(file.rev), file.value])

client.disconnect()