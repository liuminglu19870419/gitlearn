import re
import sys
import pymongo
from datetime import datetime, timedelta
#from weibonews.utils.format import datetime2microtimestamp

_NEWS_FIELDS = {
    'did' : 1,
    'news.title' : 1,
    'news.content': 1,
}

_SORT = [('_id', 1)]

_LOG_FILE = 'rebuild.log'

_CLEAN_IMG_RE = re.compile(r"<dolphinimagestart--([0-9a-fA-F]){8}-([0-9a-fA-F]){4}-([0-9a-fA-F]){4}-([0-9a-fA-F]){4}-([0-9a-fA-F]){12}--dolphinimageend>", re.IGNORECASE)
_EXTRA_HTML_TAGS_RE = re.compile(r'<(\/)?(a|b).*?>', re.IGNORECASE)

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

def rebuild(db, keep_day, last_rebuild=0):
    ''' Get news from db which is created inside keep_day, to fill dedup service
    '''
    if not inited:
        print 'dedup init failed'
        return False
    log_file = open(_LOG_FILE, 'wb')
    start = last_rebuild# or datetime2microtimestamp(datetime.utcnow() - timedelta(days=keep_day))
    cond = {'_id' : {'$gt':start}, 'type':0}
    news_count = 0
    dup_count = 0
    last_id = 0
    dedup_type = ''
    total_cost = timedelta()
    for news in db.infos.find(cond, fields=_NEWS_FIELDS, sort=_SORT):
        news_count += 1
        last_id = news['_id']
        did = int(news['did'])
        title = news['news']['title'].encode('utf8')
        content = news['news']['content'].encode('utf8')
        content = clean_content(content)
        pstart = datetime.now()
######
        pend = datetime.now()
        print 'process news %d with time %s' % (news_count, pend-pstart)
        total_cost += pend - pstart
        if dup_id > 0:
            dup_count += 1
            log_file.write("file %d's %s is duplicated with %d confident is %r\n" % (news['did'], dedup_type, dup_id, confident))
    log_file.write("Rebuild with %d news, %d duplicated found, last id %s, average cost %s\n" % (news_count, dup_count, last_id, total_cost/news_count))
    log_file.close()
    return True

def main():
    conn = pymongo.Connection(sys.argv[1])
    db = conn['weibo']
    keep_day = int(sys.argv[2])
    last = 0
    if len(sys.argv) > 3:
        last = int(sys.argv[3])
    if rebuild(db, keep_day, last):
        print 'Rebuild successed!'
    else:
        print 'Rebuild failed!'


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print 'usage: python dedup_rebuild.py db_server keep_day last_rebuild_id'
        sys.exit(0)
    main()
