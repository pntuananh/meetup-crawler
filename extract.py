#import simplejson as json
import json
import pdb
from os import listdir
from os.path import isfile
from collections import defaultdict

#dataset = 'Singapore'
#dataset = 'NYC'

#dataset = 'LA'
#dataset = 'NY'
dataset = 'CA'
#dataset = 'HK'

print dataset
def convert(s):
    try: 
        return s.encode('utf-8')
    except:
        return s

## extract events
#event_users = defaultdict(list)
#c = 0
#lines = []
#for d in ['client1\\data', 'client2\\data']:
#    for fname in listdir(d):
#        if not fname.startswith('event') : continue
#        f = open(d+'\\'+fname)
#
#        for line in f:
#            try:
#                js = json.loads(line)
#            except Exception, e:
#                continue
#
#            c += 1
#            if c % 10000 == 0:
#                print '\r%s %s %d %d' % (d,fname,len(lines),c), 
#
#            venue = js.get('venue', {})
#            city = venue.get('city', '') #.lower().strip()
#            state = venue.get('state', 'w')
#            country = venue.get('country', '').lower().strip()
#
#            #if venue.get('city', '') != 'Los Angeles': continue
#            ##if venue.get('city', '') != 'New York' : continue
#            ##if venue.get('state', '') != 'NY' : continue
#            #if venue.get('country', '').lower() != 'us' : continue
#
#            if country != 'us' : continue
#            state = state.lower().strip()
#            city = city.strip()
#            if not state:
#                if city not in ['LA', 'L.A.'] and city.lower() not in ['los angeles', 'san diego', 
#                        'san jose', 'san francisco', 'fresno', 'sacramento']: continue
#            elif state != 'ca': 
#                continue
#            else:
#                city = city.lower()
#                if not any(ct in city for ct in ['los angeles', 'san diego', 'san jose', 'san francisco', 'fresno', 'sacramento']): 
#                    continue
#
#
#            #if 'los angeles' not in city.lower():
#            #    if state.strip() != 'CA' or city.strip() not in ['LA', 'L.A.']: continue
#                
#
#            eventid = js['id']
#            venueid = venue['id']
#            lon = venue['lon']
#            lat = venue['lat']
#            event_time = js['time'] + js['utc_offset'] - 28800000
#            groupid = js['group']['id'] 
#
#            lines.append('%s %d %f %f %d %d' % (eventid, venueid, lon, lat, event_time, groupid))
#
#f = open('event_in_%s.txt' % dataset, 'w')
#f.write('\n'.join(lines))
#f.close()
#
#print len(lines)

## extract rsvp
#all_events = set()
#for line in open('event_in_%s.txt' % dataset):
#    p = line.strip('\n').split()
#    all_events.add(p[0])
#
#print len(all_events)
#event_users = defaultdict(list)
#
#c = 0
#total = 0
#for d in ['client1\\data', 'client2\\data']:
#    for fname in listdir(d):
#        if not fname.startswith('rsvp') : continue
#        f = open(d+'\\'+fname)
#
#        for line in f:
#            try:
#                js = json.loads(line)
#            except Exception, e:
#                continue
#
#            c += 1
#            if c % 1000 == 0:
#                print '\r%s %s %d %d %d' % (d,fname,total,len(event_users),c), 
#
#            results = js['results']
#            for res in results:
#                eventid = res['event']['id']
#                if eventid not in all_events: break
#
#                userid = res['member']['member_id']
#                created = res['created']
#                event_users[eventid].append((created,userid))
#
#                total += 1
#
#lines = []
#for event, time_users in event_users.iteritems():
#    lines.append('%s %s' % (event, ' '.join(str(user) for created, user in sorted(time_users))))
#f = open('event_users_%s.txt' % dataset,'w')
#f.write('\n'.join(lines))
#f.close()
#
#print '\n', len(lines)
    
## extract description
#all_events = set()
#for line in open('event_in_Singapore.txt'):
#    p = line.strip('\n').split()
#    all_events.add(p[0])
#
#c = 0
#lines = []
#stop = False
#
#for i in range(163):
#    for d in ['data1', 'data2']:
#        fname = 'event%04d.txt' % i
#        #if not fname.startswith('event') : continue
#        f = open(d+'\\'+fname)
#
#        for line in f:
#            try:
#                js = json.loads(line)
#            except Exception, e:
#                continue
#
#            c += 1
#            if c % 1000 == 0:
#                print '\r%s %s %d %d %d' % (d,fname,len(lines),c, len(all_events)), 
#
#            eventid = js['id']
#            if eventid not in all_events: continue
#            all_events.remove(eventid)
#
#            if 'description' in js:
#                #desc = convert(js['description'])
#                desc = js['description'].replace('\t','').replace('\n','')
#                try:
#                    #lines.append('%s %s' % (eventid, desc))
#                    lines.append(convert('%s %s' % (eventid, desc)))
#                except Exception, e:
#                    print e
#                    pdb.set_trace()
#
#            if not all_events:
#                stop = True
#                break
#
#        if stop: break
#    if stop: break
#
#f = open('event_desc_Singapore.txt', 'w')
#f.write('\n'.join(lines))
#f.close()

## clean description
#from HTMLParser import HTMLParser
#
#class MLStripper(HTMLParser):
#    def __init__(self):
#        self.reset()
#        self.fed = []
#    def handle_data(self, d):
#        self.fed.append(d)
#    def get_data(self):
#        return ''.join(self.fed)
#
#def strip_tags(html):
#    s = MLStripper()
#    s.feed(html)
#    return s.get_data()
#
#lines = []
#k = 0
#for line in open('event_desc_Singapore.txt'):
#    p = line.strip('\n').split(' ', 1)
#    event = p[0]
#    desc = strip_tags(p[1])
#
#    if desc:
#        lines.append(convert('%s %s') % (event, desc))
#
#    print '\r%d' % k,
#    k+=1
#
#f = open('event_clean_desc_Singapore.txt','w')
#f.write('\n'.join(lines))
#f.close()

## extract user tags
#all_users = set()
#for line in open('event_users_%s.txt' % dataset):
#    p = line.strip('\n').split()
#    event = p[0]
#    all_users.update(p[1:])
#
#print len(all_users)
#
#stop = False
#lines = []
#c = 0
#hit = 0
#for d in ['client1\\data', 'client2\\data']:
#    for fname in listdir(d):
#        if not fname.startswith('user') : continue
#        try:
#            f = open(d+'\\'+fname)
#        except:
#            continue
#
#        for line in f:
#            try:
#                js = json.loads(line)
#            except Exception, e:
#                continue
#
#            c += 1
#            if c % 10000 == 0:
#                print '\r%s %s %d %d %d' % (d,fname,len(lines),c, hit), 
#
#            userid = str(js['id'])
#            if userid not in all_users: continue
#            #all_users.remove(userid)
#            hit += 1
#            stop = hit == len(all_users)
#
#            topics = js['topics']
#            if not topics: 
#                if stop: break
#                else: continue
#
#            s = '%s %s' % (userid, ' '.join(['%d' % t['id'] for t in topics]))
#            lines.append(s)
#
#            if stop: break
#        if stop: break
#    if stop: break
#
#print '\n', len(lines)
#
#f = open('user_tags_%s.txt' % dataset, 'w')
#f.write('\n'.join(lines))
#f.close()

## extract group members
#all_users = set()
#for line in open('event_users_%s.txt' % dataset):
#    p = line.strip('\n').split()
#    event = p[0]
#    all_users.update(p[1:])
#
#all_groups = set()
#for line in open('event_in_%s.txt' % dataset):
#    p = line.strip('\n').split()
#    group = p[5]
#    all_groups.add(group)
#
#print len(all_users)
#print len(all_groups)
#
#c = 0
#k = 0
#user_onlgroups = defaultdict(list)
#for d in ['client1\\data', 'client2\\data']:
#    for fname in listdir(d):
#        if not fname.startswith('group_member'): continue
#        for line in open(d+'\\'+fname):
#            try:
#                js = json.loads(line.strip('\n'))
#            except:
#                continue
#
#            k += 1
#            if k % 100 == 0:
#                print '\r%s %s %d %d %d' % (d, fname, c, len(user_onlgroups), k),
#
#            group_id = str(js['group_id'])
#            if group_id not in all_groups: continue
#
#            for user in js['results']:
#                user_id = str(user['id'])
#
#                if user_id in all_users:
#                    user_onlgroups[user_id].append(group_id)
#
#            c += 1
#        print ''
#
#lines = []
#for user, group_list in user_onlgroups.iteritems():
#    lines.append('%s %s' % (user, ' '.join(['%s' % g for g in group_list])))
#
#print '\n', len(lines)
#f = open('user_onlgroups_%s.txt' % dataset, 'w')
#f.write('\n'.join(lines))
#f.close()

## extract group tags
all_groups = set()
for line in open('event_in_%s.txt' % dataset):
    p = line.strip('\n').split()
    group = p[5]
    all_groups.add(group)

print len(all_groups)

stop = False
lines = []
c = 0
hit = 0
for d in ['client1\\data', 'client2\\data']:
    for fname in listdir(d):
        if not fname.startswith('group') : continue
        if fname.startswith('group_member') : continue
        try:
            f = open(d+'\\'+fname)
        except:
            continue

        for line in f:
            try:
                js = json.loads(line)
            except Exception, e:
                continue

            c += 1
            if c % 10000 == 0:
                print '\r%s %s %d %d %d' % (d,fname,len(lines),c, hit), 

            if not js['results'] : continue

            js = js['results'][0]
            groupid = str(js['id'])

            if groupid not in all_groups: continue
            hit += 1
            stop = hit == len(all_groups)

            topics = js['topics']
            if not topics: 
                if stop: break
                else: continue

            s = '%s %s' % (groupid, ' '.join(['%d' % t['id'] for t in topics]))
            lines.append(s)

            if stop: break
        if stop: break
    if stop: break

print '\n', len(lines)
f = open('group_tags_%s.txt' % dataset, 'w')
f.write('\n'.join(lines))
f.close()


## extract by time
#one_hour    = 60*60
#one_day     = one_hour*24
#one_week    = one_day*7
#one_month   = one_day*30
#
#current_ts = 1356969600 
#dataset = 'NY'
#
#lines = []
#event_set = set()
#
#k = 0
#c = 0
#for line in open('event_in_%s_full.txt' % dataset):
#    p = line.strip('\n').split(' ')
#    ts = int(p[4])/1000
#    if current_ts - one_month*12 <= ts <= current_ts + one_month*12:
#        event = p[0]
#        event_set.add(event)
#        lines += [line]
#        c += 1
#
#    k += 1
#    if k % 100000 == 0:
#        print '\r%d %d' % (k,c),
#
#print ''
#f = open('event_in_%s_1.txt' % dataset, 'w')
#f.write(''.join(lines))
#f.close()
#
#lines = []
#
#k = 0
#c = 0
#for line in open('event_users_%s_full.txt' % dataset):
#    p = line.strip('\n').split(' ',1)
#    event = p[0]
#    if event in event_set:
#        lines += [line]
#        c += 1
#
#    k += 1
#    if k % 100000 == 0:
#        print '\r%d %d' % (k,c),
#
#print ''
#f = open('event_users_%s_1.txt' % dataset, 'w')
#f.write(''.join(lines))
#f.close()

## extract fee
#event_set = set()
#f = open('event_in_%s.txt' % dataset)
#for line in f:
#    event = line.split(' ',1)[0]
#    event_set.add(event)
#f.close()
#
#print len(event_set)
#
#c = 0
#lines = []
#for d in ['client1\\data', 'client2\\data']:
#    for fname in listdir(d):
#        if not fname.startswith('event') : continue
#        f = open(d+'\\'+fname)
#
#        for line in f:
#            try:
#                js = json.loads(line)
#            except Exception, e:
#                continue
#
#            c += 1
#            if c % 10000 == 0:
#                print '\r%s %s %d %d' % (d,fname,len(lines),c), 
#
#            eventid = js['id']
#            if eventid not in event_set: continue
#            #event_set.remove(eventid)
#
#            venue = js['venue']
#            venueid = venue['id']
#            lon = venue['lon']
#            lat = venue['lat']
#            event_time = js['time'] + js['utc_offset'] - 28800000
#            groupid = js['group']['id'] 
#            fee = js.get('fee')
#            if fee is None:
#                money = '-1'
#            else:
#                money = '%d_%s' % (fee['amount'],fee['currency'])
#
#
#            lines.append('%s %d %f %f %d %d %s' % (eventid, venueid, lon, lat, event_time, groupid, money))
#
#            if len(lines) == not event_set:
#                break
#        if len(lines) == not event_set:
#            break
#    if len(lines) == not event_set:
#        break
#
#f = open('event_in_%s_fee.txt' % dataset, 'w')
#f.write('\n'.join(lines))
#f.close()
#
#print len(lines)

