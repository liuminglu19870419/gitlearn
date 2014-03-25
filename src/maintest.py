# coding: UTF-8
'''

@author: mingliu
'''
import ThriftClient
import Service
import traceback
import pymongo
import re
import threading
import time
import sys
import os
import Queue
import urllib
import urllib2
reload(sys)
sys.getdefaultencoding()
sys.setdefaultencoding('utf8')


_CLEAN_IMG_RE = re.compile(r"<dolphinimagestart--([0-9a-fA-F]){8}-([0-9a-fA-F]){4}-([0-9a-fA-F]){4}-([0-9a-fA-F]){4}-([0-9a-fA-F]){12}--dolphinimageend>", re.IGNORECASE)
_EXTRA_HTML_TAGS_RE = re.compile(r'<(\/)?(a|b).*?>', re.IGNORECASE)


ip = "10.2.8.223"
port = 9002
tran = "frame"
proc = "tjson"
timeout = 15000
request_per_second = 3.
lost = 0
total = 0
total_time = 0
thread_count = 10
lock = threading.Lock()
thrift_clients = []
clients = Queue.Queue()
resource = 600

def init():
    for _ in range(resource):
        thrift_client = ThriftClient.ThriftClient(ip, port, tran, proc, timeout)
        client = Service.Client(thrift_client.get_protocol())
        thrift_clients.append(thrift_client)
        clients.put(client)

def start():
    for thrift_client in thrift_clients:
        thrift_client.connect()

def close():
    for thrift_client in thrift_clients:
        thrift_client.close()
        
def calc_time(fun):
    def _calc_time(*args, **kwargs):
        start = time.time()
        ret = fun(*args, **kwargs)
        end = time.time() - start
        try:
#             print traceback.format_exc()
            lock.acquire()
            global total_time, total
            total_time += end
            total += 1
            if (total + lost) % 500 == 0:
                print total
                print lost
                print total_time
        finally:
            lock.release()
        return ret
    return _calc_time

def calc_lost(fun):
    def _calc_lost(*args, **kwargs):
        try:
            ret = fun(*args, **kwargs)
            return ret
        except Exception, e:
            print traceback.format_exc()
            try:
                lock.acquire()
                global lost
                lost += 1
            finally:
                lock.release()
    return  _calc_lost

@calc_lost
@calc_time
def http(tit, cont):
    client = clients.get()
    param = {"title":tit, "content":cont}
    url = 'http://10.2.8.173:8083/query/keywordscount.json'
    data = urllib.urlencode(param)
    request = urllib2.Request(url,data)
    resp = urllib2.urlopen(request, timeout=timeout)
    result = resp.read()
    resp.close()
    clients.put(client)
    

@calc_lost
@calc_time
def get(tit, cont):
    client = clients.get()
    result = client.get_content_tf(tit, cont)
    print result
    clients.put(client)


def clean_content(content):
    '''
    Clean extra information in text content:
    1. remove image placeholder
    2. replace <br> with \n
    3. remove other html tags: <b>
    '''
    if not content:
        return content
    # remove image place holder
    content = _CLEAN_IMG_RE.sub('', content)
    content = content.replace('<p>', '\n')
    content = content.replace('</p>', '\n')
    content = _EXTRA_HTML_TAGS_RE.sub('', content)
    return content.strip(' \r\n')

def maketestdata():
    conn_local = pymongo.Connection()
    conn = pymongo.Connection("10.2.0.41")
    db_local = conn_local['tempd']
    db = conn['filtertest']
    for news in db.infos_12w.find():
        tit = news['news']['title'].encode('utf8')
        cont = news['news']['content'].encode('utf8')
        cont = clean_content(cont)
        data = {'title': tit, "content":cont}
        print data
        db_local.tempt.insert(data)

def main_fun():
    conn_local = pymongo.Connection()
    db_local = conn_local['tempd']
    i = 0
    init()
    start()
    threads = []
    strt = time.time()
    for news in db_local.tempt.find():
        tit = news['title']
        cont = news['content']
        i += 1
        if i % 5000 == 0:
            break
        try:
         #   tit = str(tit)
         #   cont = str(cont)
            tit = '11'
            cont = '111'
            time.sleep(1 / request_per_second)
            thread = threading.Thread(target=get, args=(tit, cont))
#             thread = threading.Thread(target=http, args=(tit, cont))
            thread.start()
            threads.append(thread)
        except Exception, e:
            print e
            print traceback.format_exc()
            print tit
    for item in threads:
        item.join()
    end = time.time() - strt
    print "global time:", end
    close()
    print "total lost:", lost
    print "calc time:", total_time

def test():
    conn_local = pymongo.Connection()
    db_local = conn_local['tempd']
    thrift_client = ThriftClient.ThriftClient(ip, port, tran, proc, timeout)
    client = Service.Client(thrift_client.get_protocol())
    thrift_client.connect()
    i = 0
    try:
        for news in db_local.tempt.find():
            if i == 0:
                i += 1
                continue
            i += 1
            if i == 12000:
                break
#             thrift_client.connect()
            tit = news['title']
            cont = news['content']
            try:
                tit = str(tit)
                cont = str(cont)
               # tit = 'fadf afdasf'
               # cont = 'fadf fasdfa werwe'
                result = client.get_content_tf(tit, cont)
              #  print tit,result
            except Exception, e:
                print e
                print traceback.format_exc()
                print tit
             #   thrift_client.close()
             #   thrift_client.connect()
    except Exception, e:
        print traceback.format_exc()
    finally:
        thrift_client.close()

def main():
    thread_list = []
    for _ in range(thread_count):
        thread_item = threading.Thread(target=test)
        thread_list.append(thread_item)
    start = time.clock()
    for thread_item in thread_list:
        thread_item.start()
    
    for thread_item in thread_list:
        thread_item.join()
    end = time.clock() - start
    print end

def singal():
    thrift_client = ThriftClient.ThriftClient(ip, port, tran, proc, timeout)
    client = Service.Client(thrift_client.get_protocol())
    thrift_client.connect()
    try:
        tit = "service Calculator extends shared.SharedService"
        cont = '''/**
   * A method definition looks like C code. It has a return type, arguments,
   * and optionally a list of exceptions that it may throw. Note that argument
   * lists and exception lists are specified using the exact same syntax as
   * field lists in struct or exception definitions.
   */

   void ping(),

   i32 add(1:i32 num1, 2:i32 num2),

   i32 calculate(1:i32 logid, 2:Work w) throws (1:InvalidOperation ouch),

   /**
    * This method has a oneway modifier. That means the client only makes
    * a request and does not listen for any response at all. Oneway methods
    * must be void.
    */
   oneway void zip()

}
'''
        try:
            print client.get_content_tf(tit, cont)
        except Exception, e:
            print e
            print tit
            print cont
            thrift_client.close()
            thrift_client.connect()
    finally:
        thrift_client.close()


if __name__ == '__main__':
#     start = time.time()
#     time.sleep(0.003)
#     end = time.time() - start
#     print end
    main_fun()
#     http("test", "test test")
#     maketestdata()
#     main()
#     singal()
