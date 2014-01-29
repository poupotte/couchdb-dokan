from couchdb import Server
from couchdb.client import Row, ViewResults

try:
    import simplejson as json
except ImportError:
    import json # Python 2.6
import os

DATABASE = "cozy-files"
SERVER = Server('http://localhost:5984/')
def replicate_from_local_ids(ids):
    '''
    Replicate metadata from local to cozy with a one-shot replication
    '''
    database = SERVER[DATABASE]
    (username, password) = _get_credentials()
    source = 'http://%s:%s@localhost:5984/%s' % (username, password, DATABASE)
    res = database.view('device/all')
    for device in res:
        device = device.value
        url = device['url'].split('/')
        target = "https://%s:%s@%s/cozy" % (device['login'], device['password'], url[2])
        print(source)
        print(target)
        SERVER.replicate(source, target, doc_ids=ids)

def _get_credentials():
    '''
    Get credentials from config file.
    '''
    #credentials_file = open('/etc/cozy/cozy-files/couchdb.login')
    #lines = credentials_file.readlines()
    #credentials_file.close()
    #username = lines[0].strip()
    #password = lines[1].strip()
    #return (username, password)
    return("test", "secret")
