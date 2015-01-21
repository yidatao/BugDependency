from bs4 import BeautifulSoup
from urllib.request import urlopen
import pymysql
from datetime import datetime
import os.path
import sys


start = datetime.strptime('2012-08-01 00:00', '%Y-%m-%d %H:%M')
end = datetime.strptime('2014-08-01 00:00', '%Y-%m-%d %H:%M')

#get the meta-data of a bug
#TODO duplicate field
#TODO only insert to db those bugs who have dependencies. But still count those who don't to compute the percentage (or simply large-small)
#TODO dependon/block bugs that are not in the crawling range
def crawl(bug_id):
    url = 'https://bugzilla.mozilla.org/show_bug.cgi?id=' + str(bug_id)
    source = urlopen(url).read().decode('utf-8')
    soup = BeautifulSoup(source)

    ##check if we can access this bug##
    error = soup.find('td',{'id':'error_msg'})
    if not error is None:
        if 'not authorized' in error.text:
            print(str(bug_id)+' not authorized')
        if 'a valid bug number' in error.text:
            print(str(bug_id)+' not valid')
        write_invalid_bug(bug_id)
        return 0

    ##depend on##
    depend_raw = soup.find('span',{'id':'dependson_input_area'}).parent.findAll('a')
    depend = []
    for d in depend_raw:
        href = d['href']
        href = href[href.find('?id=')+4:]
        depend.append(href)

    ##block##
    block_raw = soup.find('span',{'id':'blocked_input_area'}).parent.findAll('a')
    block = []
    for b in block_raw:
        #in case developers use name instead of id to refer to bug
        href = b['href']
        href = href[href.find('?id=')+4:]
        block.append(href)

    #if this bug has no dependon and no block, don't insert to db
    if len(depend) == 0 and len(block) == 0:
        return 0

    print(str(bug_id) + ' start')

    ##title of the bug##
    title_raw = soup.find('title').text.strip()
    title = title_raw[len(str(bug_id))+3:]


    ##status##
    status_raw = soup.find('span',{'id':'static_bug_status'}).text.strip().split()
    status = ' '.join(status_raw)

    ##product##
    product = soup.find('td',{'id':'field_container_product'}).text.strip()

    ##component##
    component_raw = soup.find('td',{'id':'field_container_component'}).text.strip()
    component = component_raw[:component_raw.find('(')].strip()

    ##version##
    version = soup.find('label',{'for':'version'}).parent.parent.find('td').text.strip()

    ##platform##
    platform_raw = soup.find('label',{'for':'rep_platform'}).parent.parent.find('td').text.strip()
    platform = ' '.join(platform_raw.split())

    ##importance/priority##
    priority_raw = soup.find('label',{'for':'priority'}).parent.parent.find('td').text.strip()
    priority = priority_raw.split()[1]

    ##milestone##
    milestone = soup.find('label',{'for':'target_milestone'}).parent.parent.find('td').text.strip()

    ##assignee##
    assignee_raw = soup.find('span',{'class':'fn'})
    if assignee_raw is None:
        assignee_raw = soup.find('span',{'class':'ln'})
    assignee = assignee_raw.text.strip()

    ##duplicates##
    duplicate = ''
    if 'DUPLICATE' in status:
        duplicate = status[status.rfind(' ')+1:]
    # the following parse "duplicate" field, might be over-kill
    # duplicate_raw = soup.find('span',{'id':'duplicates'})
    # if duplicate_raw is not None:
    #     dup_text = duplicate_raw.find('a')['href']
    #     dup_text = dup_text[dup_text.find('bug_id=')+7:]
    #     if ',' in dup_text:
    #         duplicate = dup_text.split(',')
    #     else:
    #         duplicate.append(dup_text)

    ##reported time##
    reported_raw = soup.find('td',{'class':'bz_show_bug_column_table'}).find('td').text.strip().split()
    reported = reported_raw[0] + ' ' + reported_raw[1]

    ##resolved time## (instead of last updated time)
    url_activity = 'https://bugzilla.mozilla.org/show_activity.cgi?id=' + str(bug_id)
    source_activity = urlopen(url_activity).read().decode('utf-8')
    soup_activity = BeautifulSoup(source_activity)

    resolve_time = ''
    cursor_time = ''
    table = soup_activity.find('div',{'id':'bugzilla-body'})
    if table.find('table') is not None:
        trs = table.find('table').findAll('tr')
        for row in trs:
            column = row.findAll('td')
            #the first row is header
            if len(column) == 0:
                continue
            if len(column) == 5:
                cursor_time = column[1].text.strip()
            what_index = 2 if len(column)==5 else 0
            what = column[what_index].text.strip()
            if what == 'Status':
                added = column[len(column)-1].text.strip()
                if added == 'RESOLVED' or added == 'FIXED' or added == 'VERIFIED':
                    resolve_time = cursor_time
        resolve_time = resolve_time[:resolve_time.rfind(':')]

    #insert into database
    insert_db(bug_id,title,status,product,component,version,platform,priority,milestone,assignee,duplicate,depend,block,reported,resolve_time)
    return 1

#append invalid or unauthorised bugs to file
def write_invalid_bug(bug):
    f = open('data/invalid_bugs','a')
    f.write(bug+'\n')
    f.close()

#get list of invalid bugs
def get_invalid_bug():
    if os.path.isfile('data/invalid_bugs'):
        return [line.strip() for line in open('data/invalid_bugs')]
    else:
        return []

#insert bugs into database
def insert_db(bug_id, title, status, product, component, version, platform, priority, milestone, assignee, duplicate, depend, block, reported_time, resolved_time):
    cur = conn.cursor()
    title = title.replace('"','')
    assignee = assignee.replace('"','')
    #assignee = assignee.replace(u'\u2039', '[').encode('latin-1')
    sql = "insert into bug_report.Metadata values (\""+str(bug_id)+"\",\""+title+"\",\""+status+"\",\""+product+"\",\""+component+"\",\""+version+"\",\""+platform+"\",\""+priority+"\",\""+milestone+"\",\""+assignee+"\",\""+duplicate+"\",\""+str(depend)+"\",\""+str(block)+"\",\""+reported_time+"\",\""+resolved_time+"\")"
    cur.execute(sql)
    conn.commit()
    cur.close()

def update_db_marktime(bug_id, depend_time, block_time):
    cur = conn.cursor()
    sql = 'update bug_report.Metadata set MarkDepend=\"' + depend_time + '\", MarkBlock=\"' + block_time + '\" where BugID=' + bug_id
    cur.execute(sql)
    conn.commit()
    cur.close()

#add the time where developers mark "dependon" and "block" TODO: this should be done in the intial crawling, but right now is for testing
def get_mark_dependency_time():
    bugs = get_bugs_in_db_without_mark()
    for b in bugs:
        print(b)
        url = 'https://bugzilla.mozilla.org/show_activity.cgi?id=' + b
        source = urlopen(url).read().decode('utf-8')
        soup = BeautifulSoup(source)

        depend_time = []
        block_time = []
        table = soup.find('div',{'id':'bugzilla-body'})
        if table.find('table') is not None:
            trs = table.find('table').findAll('tr')
            for row in trs:
                column = row.findAll('td')
                #the first row is header
                if len(column) == 0:
                    continue
                if len(column) == 5:
                    time = column[1].text.strip()
                    time = time[:time.rfind(':')]
                what_index = 2 if len(column)==5 else 0
                what = column[what_index].text.strip()
                if what == 'Depends on':
                    #the varchar length is 7000, the length of one date is around 20
                    if time not in depend_time and len(depend_time) < 350:
                        depend_time.append(time)
                if what == 'Blocks':
                    if time not in block_time and len(block_time) < 350:
                        block_time.append(time)

        update_db_marktime(b, str(depend_time), str(block_time))


def get_bugs_in_db_without_mark():
    allbugs = get_bugs_in_db()
    #get bugs that are already updated for mark dependency
    bugs = []
    cur = conn.cursor()
    cur.execute("select BugID from bug_report.Metadata where MarkBlock>\'\' or MarkDepend>\'\'")
    for row in cur.fetchall():
        bugs.append(str(row[0]))
    cur.close()

    return [b for b in allbugs if b not in bugs]

#get existing bugs
def get_bugs_in_db():
    #get all the bugs in db
    bugs_in_db = []
    cur = conn.cursor()
    cur.execute("select BugID from bug_report.Metadata")
    for row in cur.fetchall():
        bugs_in_db.append(str(row[0]))
    cur.close()
    return bugs_in_db

#test if there is any more dependent bugs that are not in db
def get_uninsert(tree, bugs_in_db):
    invalid_bugs = get_invalid_bug()
    #get all related bugs (dependson/blocks) TODO if we only consider bugs in db when build the tree, then getting all bugs from the tree makes not sense since all nodes in the tree are definitely in db
    allbugs = list(set(tree.get_nodes_with_parents())|set(tree.get_nodes_with_children()))

    uninsert = []
    for b in allbugs:
        if b not in invalid_bugs and b not in bugs_in_db:
            uninsert.append(b)
    return uninsert


#check if the bug is reported within the range (recent two years)
#but since we can achieve this also using Bugzilla's customized search, for now we can just use the resulting bug id range
def is_in_time_range(time):
    report_time = datetime.strptime(time, '%Y-%m-%d %H:%M')
    if start < report_time and report_time < end:
        return True
    return False


def create_db_metadata():
    sql = """
    create table bug_report.Metadata
    (
        BugID int,
        Title varchar(500),
        BugStatus varchar(50),
        Product varchar(50),
        Component varchar(100),
        Version varchar(50),
        Platform varchar(100),
        Priority varchar(50),
        Milestone varchar(50),
        Assignee varchar(500),
        Duplicate varchar(500),
        DependOn varchar(5000),
        Blocks varchar(5000),
        Reported varchar(50),
        Resolved varchar(50),
        PRIMARY KEY (BugID)
    )
    """
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()

if __name__ == "__main__":
    args = sys.argv
    host = args[1]
    port = args[2]
    user = args[3]
    pwd = args[4]
    conn = pymysql.connect(host=host, port=port, user=user, passwd=pwd, db='bug_report', charset='utf8')

    start = 825797 #2013-01-01
    initial_limit = 500 #if interrupt, minus the count in DB
    all_limit = 20000 #max number of crawled bugs in DB TODO do we have a better stop mechanism?
    cursor = 825797
    initial = True #whether the initial crawling is finished

    # count = 0
    # while count <= initial_limit and not initial:
    #     count += crawl(cursor)
    #     cursor += 1
    #
    # uninserted = True
    # bugs_in_db = get_bugs_in_db()
    # while uninserted and len(bugs_in_db) <= all_limit:
    #     tree = Tree()
    #     uninsert_list = get_uninsert(tree, bugs_in_db)
    #     if len(uninsert_list) == 0:
    #         uninserted = False
    #         continue
    #     else:
    #         print('# uninserted bugs: ' + str(len(uninsert_list)))
    #         for ui in uninsert_list:
    #             crawl(ui)
    #         bugs_in_db = get_bugs_in_db()
    get_mark_dependency_time()

    conn.close()


