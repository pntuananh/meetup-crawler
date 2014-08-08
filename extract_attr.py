import simplejson as json
import pdb
from os import listdir
from os.path import isfile
from collections import defaultdict

c = 0
dup = 0
all_events = set()
for d in ['client1\\data', 'client2\\data']:
    for fname in listdir(d):
        if not fname.startswith('event'):continue
        print ''
        print d, fname
        f = open(d+'\\'+fname)

        for line in f:
            try:
                js = json.loads(line)
            except Exception, e:
                continue
            
            event_id = js['id']
            if event_id in all_events:
                dup += 1
            all_events.add(event_id)

            c += 1
            if c%1000 == 0:
                print '\r%d %d' % (c, dup),

print ''
print c, dup
