#!/usr/bin/python
"""
created by stephan preeker. 2011-10
"""
import doozer
import gevent

from doozer.client import RevMismatch, TooLate, NoEntity, BadPath
from gevent import Timeout

class DoozerData():
    """
    class which stores data to doozerd backend

    locally we store the path -> revision numbers dict

    -all values need to be strings.
    -we watch changes to values.
        in case of a update we call the provided callback method with value.

    on initialization a path/folder can be specified where all keys
    will be stored.
    """

    def __init__(self, client, callback=None, path='/pydooz'):
        self.client = client
        self._folder = path
        self.revisions = {}
        #method called when external source changes value.
        self.callback = callback

        #load existing values.
        walk = client.walk('%s/**' % path)
        for file in walk:
            self.revisions[self.key_path(file.path)] = file.rev

        #start watching for changes.
        if self.callback:
            self.watch()

    def watch(self):
        """
        watch the directory path for changes.
        call callback on change.
        do NOT call callback if it is a change of our own,
        thus when the revision is the same as the rev we have
        stored in out revisions.
        """

        rev =  self.client.rev().rev

        def watchjob(rev):
            change = None

            while True:
                try:
                    change = self.client.wait("%s/**" % self._folder, rev)
                except Timeout:
                    rev = self.client.rev().rev
                    change = None

                if change:
                    self._handle_change(change)
                    rev = change.rev+1
                #print '.....', rev

        self.watchjob = gevent.spawn(watchjob, rev)

    def _handle_change(self, change):
        """
        A change has been watched. deal with it.
        """
        #get the last part of the path.
        key_path = self.key_path(change.path)

        print 'handle change', self.revisions.get(key_path, 0), change.rev

        if self._old_or_delete(key_path, change):
            return

        print change.path
        self.revisions[key_path] = change.rev
        #create or update route.
        if change.flags == 4:
            self.revisions[key_path] = change.rev
            self.callback(change.value)
            return

        print 'i could get here ...if i saw my own/old delete.'
        print change

    def _old_or_delete(self, key_path, change):
        """
        If the change is done by ourselves or already seen
        we don't have to do a thing.
        if change is a delete not from outselves call the callback.
        """
        if key_path in self.revisions:
            #check if we already have this change.
            if self.revisions[key_path] == change.rev:
                return True
            #check if it is an delete action.
            #if key_path is still in revisions it is not our
            #own or old delete action.
            if change.flags == 8:
                print 'got delete!!'
                self.revisions.pop(key_path)
                self.callback(change.value, path=key_path, destroy=True)
                return True

        return False

    def get(self, key_path):
        """
        get the latest data for path.

        get the local revision number
        if revision revnumber does not match??
           -SUCCEED and update rev number.
        return the doozer data
        """
        rev = 0
        if key_path in self.revisions:
            rev = self.revisions[key_path]
        try:
            return self.client.get(self.folder(key_path), rev).value
        except RevMismatch:
            print 'revision mismach..'
            item = self.client.get(self.folder(key_path))
            self.revisions[key_path] = item.rev
            return item.value

    def set(self, key_path, value):
        """
        set a value, BUT check if you have the latest revision.
        """
        if not isinstance(value, str):
            raise TypeError('Keywords for this object must be strings. You supplied %s' % type(value))

        rev = 0
        if key_path in self.revisions:
            rev = self.revisions[key_path]
        self._set(key_path, value, rev)

    def _set(self, key_path, value, rev):
        try:
            newrev = self.client.set(self.folder(key_path), value, rev)
            self.revisions[key_path] = newrev.rev
            print self.revisions[key_path]
            print 'setting %s with rev %s oldrev %s' % (key_path, newrev.rev, rev)
        except RevMismatch:
            print 'ERROR failed to set %s %s %s' % (key_path, rev, self.revisions[key_path])

    def key_path(self, path):
        return path.split('/')[-1]

    def folder(self, key_path):
        return "%s/%s" % (self._folder, key_path)

    def delete(self, key_path):
        """
        delete path. only with correct latest revision.
        """
        try:
            rev = self.revisions[key_path]
            self.revisions.pop(key_path)
            item = self.client.delete(self.folder(key_path), rev)
        except RevMismatch:
            print 'ERROR!! rev value changed meanwhile!!', item.path, item.value
        except BadPath:
            print 'ERROR!! path is bad.', self.folder(key_path)

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

        for thing in folder:
            item = self.client.get(self.folder(thing.path))
            yield (thing.path, item.rev, item.value)


def print_change(change, path=None, destroy=True):
    print 'watched a change..'
    print  change, destroy, path

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
    d.set('foo1', 'bar1')
    d.set('foo2', 'bar2')
    d.set('foo3', 'bar3')
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
    # because the change_value job adds data later.
    cv.join(cv)
    #d.delete_all()

if __name__ == '__main__':
    test_doozerdata()

