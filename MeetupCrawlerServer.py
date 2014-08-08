import tornado.ioloop
import tornado.web
import simplejson as json
from collections import deque
import time, os
import sys, getopt
import pdb

MAX_RETURN_ITEM = 100
current_clients = 0
queues = {}
seen = {}

for typ in ['group','user','event']:
    queues[typ] = deque()
    seen[typ] = set()

queues['downloaded_user'] = deque()
seed_groups = [12941052,3952812,8648022,5077782,12941452,11168062]
#queues['group'].append(8805942) #1647796)
queues['downloaded_group'] = deque()

class InitHandler(tornado.web.RequestHandler):
    def get(self):
        global current_clients
        
        print time.ctime(), 'INIT request received'
        
        current_clients += 1
#        self.write('{"res": %d}' % current_clients)
        self.write(str(current_clients))

        
class GetHandler(tornado.web.RequestHandler):
    def get(self):
        global queues
        
        response = []
        for typ in ['user', 'event']:
            num = min(MAX_RETURN_ITEM-len(response),len(queues[typ]))
            for _ in range(num):
                if typ == 'user':
                    user_id = queues[typ].popleft()
                    queues['downloaded_user'].append(user_id)
                    response += [(typ, user_id)]
                else:
                    response += [(typ, queues[typ].popleft())]
                
            if len(response) == MAX_RETURN_ITEM:
                break
        
        if not response:
            for typ in ['group','downloaded_group','downloaded_user']:
                if queues[typ]:
                    response = [(typ, queues[typ].popleft())]
                    break
            
        stat = {}
        for typ, _ in response:
            stat[typ] = stat.get(typ,0) + 1
        print time.ctime(), 'GET', stat

        self.write(json.dumps(response))
        
        
class PutHandler(tornado.web.RequestHandler):
    def post(self):
        global queues, seen

        stat = {}
        js = json.loads(self.request.body)
        for typ, item in js:
            stat[typ] = stat.get(typ,0) + 1
            if item not in seen[typ]:
                queues[typ].append(item)
                seen[typ].add(item)

        print time.ctime(), 'POST', stat
        self.write('{"res": "ok"}')


application = tornado.web.Application([
    (r"/init", InitHandler),                                   
    (r"/get", GetHandler),
    (r"/put", PutHandler),
])

def reload_entity(current_dir, typ):
    i = 0
    while True:
        filename = current_dir + '%s%04d.txt' % (typ,i)
        if not os.path.exists(filename):
            break

        print '\r%s' % filename,
        for line in open(filename):
            line = line.strip('\n')
            try:
                entity = json.loads(line)

                if typ == 'group':
                    entity_id = entity['results'][0]['id']
                elif typ == 'user':
                    entity_id = entity['id']
                elif typ == 'event':
                    entity_id = entity['id']

                seen[typ].add(entity_id)

                if typ == 'user':
                    queues['downloaded_user'].append(entity_id)
                elif typ == 'group':
                    queues['downloaded_group'].append(entity_id)

            except:
                continue
        i += 1


def reload(numdir):
    if numdir:
        for typ in ['group','user','event']:
            print 'Reloading %ss...' % typ
            for d in range(1,numdir+1):
                current_dir = 'client%d\\data\\' % d
                reload_entity(current_dir, typ)
            print ''
            print 'Reloaded %ss: %d items' % (typ, len(seen[typ]))

    print 'Finished reloading!'
    for g in seed_groups:
        if g not in seen['group']:
            queues['group'].append(g)
            seen['group'].add(g)


if __name__ == "__main__":
    numdir = 0
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'c:')
    except getopt.GetoptError:
        print 'MeetupCarwlerServer.py [-c n]'
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            print 'MeetupCarwlerServer.py [-c n]'
            sys.exit()
        elif opt == '-c':
            numdir = int(arg)

    reload(numdir)
    application.listen(8888)
    tornado.ioloop.IOLoop.instance().start()
