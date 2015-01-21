from bs4 import BeautifulSoup
from urllib.request import urlopen
import pymysql, sys


workaround_keywords = [' workaround',' work around',' work-around',' temporary solution',' temporary fix',' hack',' quick fix']

def has_workaround(bug_id):
    url = 'https://bugzilla.mozilla.org/show_bug.cgi?id=' + str(bug_id)
    source = urlopen(url).read().decode('utf-8')
    soup = BeautifulSoup(source)

    ##attachment##
    id = 1 # 0 is the attachment title, ignore
    has_attach = False
    attach = soup.find('table',{'id':'attachment_table'}).find('tr',{'id':'a'+str(id)})
    while attach is not None:
        attach_info = attach.find('span',{'class':'bz_attach_extra_info'}).text.strip()
        #attachment should be a patch, instead of img, html, etc.
        if 'patch' in attach_info:
            has_attach = True
            break
        id += 1
        attach = soup.find('table',{'id':'attachment_table'}).find('tr',{'id':'a'+str(id)})

    if not has_attach:
        return False


    ##comments##
    comments = []
    comment_elem = soup.find('div',{'id':'comments'}).find('table',{'class':'bz_comment_table'})
    for c in comment_elem.findAll('div',{'class':'bz_comment'}):
        elem = c.find('pre',{'class':'bz_comment_text'}).text.strip()
        for k in workaround_keywords:
            if k in elem.lower():
                comments.append(elem)

    if len(comments) > 0:
        return True
    return False


if __name__ == "__main__":
    args = sys.argv
    host = args[1]
    port = args[2]
    user = args[3]
    pwd = args[4]
    conn = pymysql.connect(host=host, port=port, user=user, passwd=pwd, db='bug_report', charset='utf8')
    #print(has_workaround('3655'))
    #get all the bugs in db
    existing = []
    cur = conn.cursor()
    cur.execute("select BugID from bug_report.Metadata")
    for row in cur.fetchall():
        existing.append(str(row[0]))
    cur.close()

    content = ''
    count = 0
    for bug in existing:
        if has_workaround(bug):
            content += bug + '\n'
            count += 1
            print(bug)
    print('Total bugs with workaround patch: ' + str(count))
    f = open('data/has_workaround_bugs','w')
    f.write(content)
    f.close()