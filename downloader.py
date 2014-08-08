import simplejson as json
import urllib, urllib2, time
from os import listdir

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

fout = open('missing_groups_raw_data.txt', 'w')
for line in open('missing_groups.txt'):
    c += 1
    group_id = line.strip('\n')

    query = {'group_id': group_id}
    s = download(URL_GROUP, query)
    if s:
        fout.write('%s' %s)

fout.close()
'''
c = 0
for datadir in ['data1', 'data2']:
    for filename in listdir(datadir):
        if not filename.startswith('event'): continue
        print '\r%s %d' % (datadir + '\\' + filename, len(all_groups)),
        for line in open(datadir + '\\' + filename):
            try:
                js = json.loads(line.strip('\n'))
            except:
                continue

            groupid = js['group']['id']
            all_groups.add(groupid)

print ''

seen_groups = set()
c = 0
for datadir in ['data1', 'data2']:
    for filename in listdir(datadir):
        if not filename.startswith('group0'): continue
        print '\r%s %d' % (datadir + '\\' + filename, len(seen_groups)),
        for line in open(datadir + '\\' + filename):
            try:
                js = json.loads(line.strip('\n'))
            except:
                continue

            groupid = js['results'][0]['id']
            seen_groups.add(groupid)

            #c += 1
            #if c%10000 == 0:
            #    print '\r%d %d' % (c, len(seen_groups)),
print ''

missing_groups = seen_groups - all_groups
print len(missing_groups)

f = open('missing_groups.txt', 'w')
f.write('\n'.join(['%d' % g for g in missing_groups]))
f.close()
'''
