import nltk
import nltk.corpus 
import pdb
from collections import defaultdict, Counter
from os import listdir
import json

#stopwords = set(nltk.corpus.stopwords.words('english'))
##stopwords = set(open('stopwords.txt').read().split('\n'))
#stopwords.update(['http', '://', 'www', 'com'])
#punctuations = set('!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~')
#
#
#porter = nltk.PorterStemmer()
#pattern = '\w+|[^\w\s]+'
#
#def to_term_sequence(doc):
#    tokens = [word for word in nltk.regexp_tokenize(doc, pattern) if word]
#    return normalize_term_vector(tokens)
#    #return tokens
#
#def lemmatize(word):
#    if len(word) > 3 and '-' not in word:
#        return porter.stem(word)
#    return word 
#
#def normalize(word):
#    word = word.lower()
#    return '' if (word in stopwords or word in punctuations) else word
#
#def normalize_term_vector(term_vector):
#    tokens = [normalize(word) for word in term_vector]
#    return [lemmatize(word) for word in tokens if word]
#
#doc_words = {}
#word_docs = defaultdict(list)
#k = 0
#for line in open('event_clean_desc_Singapore.txt'):
#    p = line.strip('\n').split(' ', 1)
#
#    event = p[0]
#    doc = p[1]
#    term_seq = to_term_sequence(doc)
#
#    doc_words[event] = term_seq
#    for term in term_seq:
#        word_docs[term].append(event)
#
#    print '\r%d' % k,
#    k += 1
#
#f = open('event_words.txt', 'w')
#f.write('\n'.join(['%s %s' % (event, ' '.join(words)) for event, words in doc_words.iteritems()]))
#f.close
#
#f = open('word_events.txt', 'w')
#f.write('\n'.join(['%s %s' % (word, ' '.join(events)) for word, events in word_docs.iteritems()]))
#f.close

#tag_cnt = Counter()
#user_tags = {}
#tag_users = defaultdict(list)
#
##for line in open('user_tags_Singapore.txt'):
#for line in open('user_tags_NYC.txt'):
#    p = line.strip('\n').split()
#
#    user = p[0]
#    tag_cnt.update(p[1:])
#    user_tags[user] = set(p[1:])
#
#    for tag in p[1:]:
#        tag_users[tag].append(user)
#
#print len(user_tags), len(tag_cnt)
#
#stoptags = set([t for t,c in tag_cnt.most_common(10)])
#
#for user in user_tags:
#    user_tags[user] -= stoptags
#
#user_user_sim = defaultdict(lambda: defaultdict(float))
#c = 0
#stop = False
#for tag in tag_users:
#    if tag in stoptags: continue
#    for i in range(len(tag_users[tag])-1):
#        u1 = tag_users[tag][i]
#        tag_list1 = user_tags[u1]
#        if not tag_list1: continue
#        for j in range(i+1,len(tag_users[tag])):
#            u2 = tag_users[tag][j]
#            tag_list2 = user_tags[u2]
#            if not tag_list2: continue
#
#            #if u1 in user_user_sim and u2 in user_user_sim[u1]: continue
#
#            sim = len(tag_list1 & tag_list2)*1.0/len(tag_list1 | tag_list2)
#            
#            if u2 not in user_user_sim[u1]:
#                if len(user_user_sim[u1]) == 10:
#                    lowest_sim = min(user_user_sim[u1].itervalues())
#                if len(user_user_sim[u1]) < 10 or sim > lowest_sim :
#                    user_user_sim[u1][u2] = sim
#                    if len(user_user_sim[u1]) > 10:
#                        lowest_key = [key for key,si in user_user_sim[u1].iteritems() if si == lowest_sim][0]
#                        user_user_sim[u1].pop(lowest_key)
#
#            if u1 not in user_user_sim[u2]:
#                if len(user_user_sim[u2]) == 10:
#                    lowest_sim = min(user_user_sim[u2].itervalues())
#                if len(user_user_sim[u2]) < 10 or sim > lowest_sim :
#                    user_user_sim[u2][u1] = sim
#                    if len(user_user_sim[u2]) > 10:
#                        lowest_key = [key for key,si in user_user_sim[u2].iteritems() if si == lowest_sim][0]
#                        user_user_sim[u2].pop(lowest_key)
#
#            #user_user_sim[u1][u2] = user_user_sim[u2][u1] = len(tag_list1 & tag_list2)*1.0/len(tag_list1 | tag_list2)
#
#            c += 1
#            if c % 100000 == 0:
#                print '\r%d' % c,
#
#lines = []
#for user1 in user_user_sim:
#    sorted_list = sorted([(sim,user2) for user2, sim in user_user_sim[user1].iteritems()], reverse=True)[:10] 
#
#    lines.append('%s %s' % (user1, ' '.join(['%s|%0.5f' % (u,s) for s,u in sorted_list])))
#
##f = open('user_sim_by_tags_Singapore.txt','w')
#f = open('user_sim_by_tags_NYC.txt','w')
#f.write('\n'.join(lines))
#f.close()

#tag_text = defaultdict(int)
#c = 0
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
#                print '\r%s %s %d %d' % (d,fname,len(tag_text),c), 
#
#            userid = str(js['id'])
#
#            topics = js['topics']
#            if not topics: 
#                continue
#
#            for t in topics:
#                #tag_text[t['id']] = t['name']
#                tag_text[(t['id'],t['name'])] += 1
#
#c = 0
#for d in ['client1\\data', 'client2\\data']:
#    for fname in listdir(d):
#        if not fname.startswith('group') : continue
#        if fname.startswith('group_member') : continue
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
#                print '\r%s %s %d %d' % (d,fname,len(tag_text),c), 
#
#            if not js['results'] : continue
#
#            js = js['results'][0]
#
#            topics = js.get('topics', [])
#            if not topics: 
#                continue
#
#            for t in topics:
#                if 'id' not in t or 'name' not in t: continue
#                #tag_text[t['id']] = t['name']
#                tag_text[(t['id'],t['name'])] += 1
#
#
#def convert(s):
#    try: 
#        return s.encode('utf-8')
#    except:
#        return s
#
#sorted_list = sorted([(c,i,n) for (i,n),c in tag_text.iteritems()], reverse=True)
#f = open('tag_names.txt','w')
#lines = [convert('%d :: %s :: %d' % (i,n,c)) for c,i,n in sorted_list]
#f.write('\n'.join(lines))
#f.close()

import re
from nltk.stem import PorterStemmer
stemmer = PorterStemmer()

short_tags = {}
long_tags = []
inverted_index = {}

stopwords = set(nltk.corpus.stopwords.words('english'))
delim = ' |, | & '
def tag_to_term_list(tag):
    terms = [t.lower() for t in re.split(delim, tag)]
    terms = [stemmer.stem(t) for t in terms if t not in stopwords]

for line in open('tag_names.txt'):
    tag_id, tag_name, count = line.strip('\n').split(' :: ')

    if int(count)< 100 : break

    tag_id = int(tag_id)
    terms = tag_to_term_list(tag)

    if len(terms )

