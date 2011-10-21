"""
created by stephan preeker. 2011-10
"""
import doozer
import gevent

from doozer.client import RevMismatch, TooLate, NoEntity
from gevent import Timeout

class DoozerData():
    """
    class which stores data to doozerd backend

    locally we store the path -> revision numbers dict

    -all values need to be strings.
    -we watch changes to values.
        in case of a update we call the provided callback method.

    on initialization a path/folder can be specified where all keys
    will be stored.
    """

    def __init__(self, client, callback=None, path='/pydooz'):
        self.client = client
        self._folder = path
        self.revisions = {}
        #method called when external source changes value.
        self.callback = callback

        try:
            self.pathrev = client.set('%s%s' % (path, '/doozerdata'), 'this is python data', 0).rev
        except RevMismatch:
            # datapath already exists in doozer.
            # load rev values.
            for path, rev, _ in self.items():
                self.revisions[path] = rev

        #start watching for changes.
        self.watch()

    def watch(self):
        """
        watch the directory path for changes.
        call callback on change.
        """

        rev =  self.client.rev().rev

        def watchjob(rev):
            try:
                change = self.client.wait("%s/**" % self._folder, rev)
                if change:
                    self.revisions[change.path] = change.rev
                if self.callback and change and change.flags == 4:
                    self.callback(change)
                elif change and change.flags == 8:
                    self.revisions.pop(change.path)
                watchjob(rev+1)
            except Timeout:
                return watchjob(rev+1)

        self.watchjob = gevent.spawn(watchjob, rev)

    def get(self, path):
        """get the path.."""
        #get the local revision number
        #what if revision revnumber do not match??
        #   -SUCCEED and update rev number.
        #return the doozer data
        rev = 0
        if path in self.revisions:
            rev = self.revisions[path]
        try:
            return self.client.get(self.folder(path), rev).value
        except RevMismatch:
            print 'revision mismach..'
            item = self.client.get(self.folder(path))
            self.revisions[path] = item.rev
            return item.value

    def set(self, path, value):
        """
        set a value, BUT check if you have the latest revision.
        """
        if not isinstance(value, str):
            raise TypeError('Keywords for this object must be strings. You supplied %s' % type(value))

        rev = 0
        if path in self.revisions:
            rev = self.revisions[path]
        self._set(path, value, rev)

    def _set(self, path, value, rev):
        try:
            newrev = self.client.set(self.folder(path), value, rev)
            print newrev
            self.revisions[path] = newrev.rev
        except RevMismatch:
            print 'failed to set %s %s %s' % (path, value, rev)

    def folder(self, path):
        return "%s/%s" % (self._folder, path)

    def delete(self, path):
        """
        delete path. only with correct latest revision.
        """
        try:
            rev = self.revisions[path]
            item = self.client.delete(self.folder(path), rev)
        except RevMismatch:
            item = self.client.delete(self.folder(path))
            print 'value changed meanwhile!!', item.path, item.value

    def delete_all(self):
        """ clear all data.
        """
        for path, rev, value in self.items():
            try:
                item = self.client.delete(self.folder(path), rev)
            except RevMismatch:
                item = self.client.delete(self.folder(path))
                print 'value changed meanwhile!!', item.path, item.value
            except TooLate:
                print 'too late..'
                rev = self.client.rev().rev
                item = self.client.delete(self.folder(path), rev)

    def items(self):
        """
        return all current items from doozer.
        update local rev numbers.
        """
        try:
            folder = self.client.getdir(self._folder)
        except NoEntity:
            print 'we are empty'
            folder = []

        result = []
        for thing in folder:
            item = self.client.get(self.folder(thing.path))
            result.append((thing.path, item.rev, item.value))

        return result

def print_change(change):
    print 'watched a change..'
    print  change

def change_value(d):

    gevent.sleep(1)
    d.set('test', '0')
    gevent.sleep(1)
    d.set('test2', '0')
    gevent.sleep(1)
    d.set('test', '1')

#make sure you start doozerd(s).
def test_doozerdata():

    client = doozer.connect()
    d = DoozerData(client, callback=print_change)
    print '...'
    d.set('foo1', 'bar1')
    d.set('foo2', 'bar2')
    d.set('foo3', 'bar3')
    print '...'
    #create a second client

    client2 = doozer.connect()
    d2 = DoozerData(client2, callback=print_change)
    d2.set('foo4', 'bar4')
    #let the second client change values to
    #those should be printed.
    cv = gevent.spawn(change_value, d2)

    for path, rev, value in d.items():
        print path,'->', value

    print d.get('foo1')
    print d.get('foo2')

    d.delete_all()

    #should be empty.
    for di in d.items():
        print di

    #the change value function added content over time..
    gevent.sleep(3)
    print 'data in d1'
    for di in d.items():
        print di
    print 'data in d2'
    for dii in d2.items():
        print dii
    # there is content. in both instances.
    # because the change_value job adds data later..
    cv.join(cv)
    d.delete_all()

test_doozerdata()
