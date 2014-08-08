import simplejson as json
import urllib, urllib2, time
from os import listdir
import pdb

all_groups = set()

private_key = '3a4d4365a364d4b324117792c5121'
HOST = 'https://api.meetup.com'
URL_GROUP   = HOST + '/2/groups' #&group_id=1655350''
URL_MEMBERS_OF_GROUP = HOST + '/2/members' #&group_id=1655350&page=100&offset=0'

c = 0
def download(url_type, query={}):
    query['key'] = private_key
    query = urllib.urlencode(query)
    url = '%s?%s' % (url_type, query)
    print c, url
    retry = 1
    while True:
        try:
            req = urllib2.Request(url=url, headers={'Accept-Charset': 'utf-8'})
            return urllib2.urlopen(url=req, timeout=5*60).read()
        except urllib2.HTTPError, e:
            print e
            if e.code in [401,404]:
                break
            if e.code == 400:
                time.sleep(60)
                continue
            #time.sleep(retry*TIME_TO_SLEEP)
        except Exception, e:
            print e
            #time.sleep(retry*TIME_TO_SLEEP)
            
        retry += 1
        if retry == 4:
            break

        time.sleep(retry*60)
    
    return None

fout = open('missing_groups_members_raw_data.txt', 'w')
for line in open('missing_groups.txt'):
    c += 1
    group_id = line.strip('\n')
    page = 100
    offset = 0

    while True:
        query = {
                'group_id' : group_id,
                'page' : page,
                'offset' : offset,
                }
        s = download(URL_MEMBERS_OF_GROUP, query)
        if s:
            js = json.loads(s)
            if not js['results']: break
            js['group_id'] = group_id
            fout.write('%s\n' % json.dumps(js, encoding='utf-8'))
        offset += 1

fout.close()
