import urllib, urllib2, time
import simplejson as json
import os, glob
import sys, getopt

from collections import deque
import pdb
          
private_key = ''
MEETUP_CRAWLER_SERVER = 'http://127.0.0.1:8888'
TO_FLUSH = 100
MAX_N_ITEM = 10000
TIME_TO_SLEEP = 30

HOST = 'https://api.meetup.com'
URL_GROUP   = HOST + '/2/groups' #&group_id=1655350''
URL_USER    = HOST + '/2/member/'
URL_EVENT   = HOST + '/2/event/'
URL_MEMBERS_OF_GROUP = HOST + '/2/members' #&group_id=1655350&page=100&offset=0'
URL_EVENTS_OF_GROUP = HOST + '/2/events' #&status=past&group_id=1655350&fields=event_hosts&page=3
URL_RSVP    = HOST + '/2/rsvps' #&rsvp=yes&event_id=117489742&fields=host
 

class MeetupCrawlerClient:
    def __init__(self, client_id):
        if client_id:
            self.myid = client_id
            reload = True
        else:
            self.request_id()
            reload = False
        
        print 'Reload:', reload
        print 'My id is', self.myid
        self.PATH = 'client%d\\' % self.myid
        self.DATA_DIR = self.PATH + 'data\\'
        
        if not os.path.exists(self.DATA_DIR):
            os.makedirs(self.DATA_DIR)

        self.queues = {}
        self.nu = {}
        self.files = {}
        
        for typ in ['group','user','event','downloaded_user','downloaded_group']:
            self.queues[typ] = deque()
            
        for typ in ['group','user','event','group_member','rsvp']:
            if not reload:
                self.nu[typ] = 0
                self.files[typ] = self.open_file(typ, 0)
            else:
                print typ
                max_index = -1
                for filename in glob.glob(self.DATA_DIR + typ + '[0-9]*.txt'):
                    index = int(filename[-8:-4]) 
                    max_index = max(max_index, index)

                if max_index < 0:
                    self.nu[typ] = 0
                    self.files[typ] = self.open_file(typ, 0)
                else:
                    self.nu[typ] = max_index * MAX_N_ITEM
                    for line in self.open_file(typ, max_index, 'r'): 
                        try:
                            #tmp = json.loads(line)
                            self.nu[typ] += 1
                        except:
                            pass

                    self.files[typ] = self.open_file(typ, max_index, 'a')
            
                print max_index
                print self.nu[typ]
                print ''
            
        self.err_log = open(self.PATH + 'errors.txt', 'w')
        self.url_log = open(self.PATH + 'url.txt', 'w')
        self.num_url_log = 0
        
    
    def request_id(self):
        req = urllib2.Request(MEETUP_CRAWLER_SERVER + '/init')
        self.myid = int(urllib2.urlopen(req).read())
        
        
    def open_file(self, prefix, index, mode='w'):
        f = open(self.DATA_DIR + '%s%04d.txt' % (prefix,index), mode)
        return f
    
    
    def write(self, typ, item):
        self.files[typ].write('%s' % item)
        if item[-1] != '\n':
            self.files[typ].write('\n')
        #self.files[typ].flush()
        
        nu = self.nu[typ] = self.nu[typ] + 1  
        if nu % MAX_N_ITEM == 0:
            self.files[typ].close()
            self.files[typ] = self.open_file(typ, nu/MAX_N_ITEM)
        elif nu % TO_FLUSH == 0:
            self.files[typ].flush()
            
    
    def add(self, typ, item):
        data = [(typ,item)]
        res = urllib2.urlopen(MEETUP_CRAWLER_SERVER + '/put',json.dumps(data)).read()
        return res    
    
    
    def add_list(self, typ, items):
        data = [(typ,item) for item in items]
        res = urllib2.urlopen(MEETUP_CRAWLER_SERVER + '/put',json.dumps(data)).read()
        return res
    
    
    def pop(self):
        total = 0
        for typ in ['user', 'event', 'group', 'downloaded_group', 'downloaded_user']:
            total += len(self.queues[typ])
            
        if not total:
            if not self.request_to_get_items():
                return None,None
           
        for typ in ['user', 'event', 'group', 'downloaded_group', 'downloaded_user']:
            if self.queues[typ]:
                return self.queues[typ].popleft(), typ
    
    
    def request_to_get_items(self):
        req = urllib2.Request(MEETUP_CRAWLER_SERVER + '/get')
        s = urllib2.urlopen(req).read()
        js = json.loads(s)
        if js:
            for typ, item in js:
                self.queues[typ].append(item)
            return True
        
        return False
            
          
    def download(self, url_type, query={}):
        query['key'] = private_key
        query = urllib.urlencode(query)
        url = '%s?%s' % (url_type, query)
        retry = 1
        while True:
            try:
                self.log_url(url)

                req = urllib2.Request(url=url, headers={'Accept-Charset': 'utf-8'})
                return urllib2.urlopen(url=req, timeout=5*60).read()
#                return urllib.urlopen(url).read()
            except urllib2.HTTPError, e:
                print url
                print e
                self.log('[%s - %s]' % (url,str(e)))
                if e.code in [401,404]:
                    break
                if e.code == 400:
                    time.sleep(5*60)
                    continue
                #time.sleep(retry*TIME_TO_SLEEP)
                
            except Exception, e:
                print url
                print e
                self.log('[%s - %s]' % (url,str(e)))
                #time.sleep(retry*TIME_TO_SLEEP)
                
            retry += 1
            if retry == 4:
                break

            time.sleep(retry*TIME_TO_SLEEP)
        
        return None
        
             
#    def get_group(self):
#        queue = self.queues['group']
#        if not len(queue): return False
#    
#        group_id = queue.popleft()

    def get_group(self, group_id):
        query = {'group_id': group_id}
        s = self.download(URL_GROUP, query)
        if s:
            self.write('group', s)
        
        '''Get members of the group'''
        page = 100
        offset = 0
        while True:
            query = {
                    'group_id' : group_id,
                    'page' : page,
                    'offset' : offset,
                    }
            
            s = self.download(URL_MEMBERS_OF_GROUP, query)
            if not s: break
            
            js = json.loads(s)
            if not js['results']: break
            
            ids = [res['id'] for res in js['results']]
            self.add_list('user', ids)
                
            js['group_id'] = group_id
            self.write('group_member', json.dumps(js, encoding='utf-8'))
            
            offset += 1
        
        '''Get events of the group'''
        page = 100
        offset = 0
        while True:
            query = {
                    'group_id' : group_id,
                    'status' : 'past',
                    'fields' : 'event_hosts',
                    'page' : page,
                    'offset' : offset,
                    }
            
            s = self.download(URL_EVENTS_OF_GROUP, query)
            if not s: break

            js = json.loads(s)
            if not js['results']: break

            ids = [res['id'] for res in js['results']]
            self.add_list('event', ids)
            
            offset += 1
        
        return True
    
        
#    def get_user(self):
#        queue = self.queues['user']
#        if not len(queue): return False
        
#        user_id = queue.popleft()

    def get_user(self, user_id):
        s = self.download(URL_USER + str(user_id))
        if s:
            self.write('user', s)
        
        #'''Get groups that this user belongs to'''
        #query = {'member_id': user_id}
        #s = self.download(URL_GROUP, query)
        #if s:
        #    js = json.loads(s)
        #    ids = [res['id'] for res in js['results']]
        #    self.add_list('group', ids)
            
        return True
    
    
#    def get_event(self):
#        queue = self.queues['event']
#        if not len(queue): return False
#        
#        event_id = queue.popleft()

    def get_event(self, event_id):
        query = {'fields' : 'event_hosts'}
        s = self.download(URL_EVENT + event_id, query)
        if s:
            self.write('event', s)
        
        '''Get users who joined the event'''
        query = {
                'event_id' : event_id,
                'fields' : 'host',
                'rsvp' : 'yes'
                }

        s = self.download(URL_RSVP, query)
        if s:
            self.write('rsvp', s)
            
            js = json.loads(s)
            ids = [res['member']['member_id'] for res in js['results']]
            self.add_list('user', ids)
          
        return True
    

    def get_groups_of_user(self, user_id):
        '''Get groups that this user belongs to'''
        query = {'member_id': user_id}
        s = self.download(URL_GROUP, query)
        if s:
            js = json.loads(s)
            ids = [res['id'] for res in js['results']]
            self.add_list('group', ids)

          
    def get_events_of_groups(self, group_id):
        '''Get events of this group'''
        page = 100
        offset = 0
        while True:
            query = {
                    'group_id' : group_id,
                    'status' : 'past',
                    'fields' : 'event_hosts',
                    'page' : page,
                    'offset' : offset,
                    }
            
            s = self.download(URL_EVENTS_OF_GROUP, query)
            if not s: break

            js = json.loads(s)
            if not js['results']: break

            ids = [res['id'] for res in js['results']]
            self.add_list('event', ids)
            
            offset += 1
        

    def log(self, msg):
        self.err_log.write('%s - %s\n' % (time.ctime(), msg))   
        self.err_log.flush()
         

    def log_url(self, url):
        if self.num_url_log == 1000:
            self.url_log.close()
            self.url_log = open(self.PATH + 'url.txt', 'w')
            self.num_url_log = 0

        self.num_url_log += 1
        self.url_log.write('%s - %s\n' % (time.ctime(), url))   
        self.url_log.flush()
        

    def run(self):
        getter = {
                  'group' : self.get_group,
                  'user' : self.get_user,
                  'event' : self.get_event,
                  'downloaded_user' : self.get_groups_of_user,
                  'downloaded_group' : self.get_events_of_groups,
                  }
        
        last = time.time()
        while True:
            item, typ = self.pop()
            if not item:
                time.sleep(5)
            else:
                try:
                    getter[typ](item)
                except Exception, e: 
                    print e
                    self.log(str(e))

            
            current = time.time()
            if current - last > 5:
                print time.ctime()
                print 'Downloaded groups:', self.nu['group']
                print 'Downloaded users:', self.nu['user']
                print 'Downloaded events:', self.nu['event']
                
#                print 'Group queue size:', len(self.queues['group'])#self.inqueue['group']
#                print 'User queue size:', len(self.queues['user'])#self.inqueue['user']
#                print 'Event queue size:', len(self.queues['event'])#self.inqueue['event']
                
                print '' 
                last = current
        print 'Done'
        
if __name__ == "__main__":        
    client_id = 0
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'c:')
    except getopt.GetoptError:
        print 'MeetupCarwlerClient.py [-c n]'
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            print 'MeetupCarwlerClient.py [-c n]'
            sys.exit()
        elif opt == '-c':
            client_id = int(arg)

    crawler = MeetupCrawlerClient(client_id)
    crawler.run()
