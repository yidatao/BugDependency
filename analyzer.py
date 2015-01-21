import pymysql
from datetime import datetime
import math
from collections import Counter
import os, sys
import numpy as np


def query_db_field(field, bug):
    priority_levels = ['enhancement','trivial','minor','normal','major','critical','blocker']
    cur = conn.cursor()
    sql = 'select ' + field + ' from bug_report.Metadata where BugID='+bug
    cur.execute(sql)
    result = -1
    rs = cur.fetchone()
    if rs is not None:
        if field == 'Priority':
            result = priority_levels.index(rs[0])
        elif field == 'Resolved' or field == 'Product' or field == 'Component' or field == 'Version' or field == 'Platform' or field == 'Reported' or field == 'MarkBlock' or field == 'MarkDepend':
            result = str(rs[0])
        elif field == 'Reported,Resolved':
            result = str(rs[0]) + '--' + str(rs[1])

    cur.close()
    return result


#get the statistics of heights of roots
def stats_roots_height(tree):
    heights = []
    roots = tree.get_roots()
    print('# of roots: ' + str(len(roots)))
    for r in roots:
        heights.append(tree.height(r))
    print_stats('root height',heights)

#get the statistics of the width of node
def stats_children_width(tree):
    width = []
    nodes = tree.get_nodes_with_children()
    for n in nodes:
        width.append(len(tree.get_children(n)))
    print_stats('children width',width)

#get the statistics of the width of parents
def stats_parents_width(tree):
    width = []
    nodes = tree.get_nodes_with_parents()
    for n in nodes:
        width.append(len(tree.get_parents(n)))
    print_stats('parents width',width)

#analyze the bug priority
def priority_root_leaf(tree):
    roots = tree.get_roots()
    leaves = tree.get_leaves()
    roots_priority = []
    leaves_priority =[]
    for r in roots:
        roots_priority.append(query_db_field('Priority',r))
    for l in leaves:
        leaves_priority.append(query_db_field('Priority',l))
    write_data('data/roots_priority.csv',roots_priority)
    write_data('data/leaf_priority.csv',leaves_priority)

def height_priority_correlation(tree):
    content = 'height,priority\n'
    nodes = tree.get_all_nodes()
    for n in nodes:
        priority = query_db_field('Priority',n)
        if priority != -1:
            content += str(tree.height(n)) + ',' + str(priority) + '\n'

    f = open('data/height_priority.csv','w')
    f.write(content)
    f.close()

def width_priority_correlation(tree):
    content = 'width,priority\n'
    nodes = tree.get_all_nodes()
    for n in nodes:
        priority = query_db_field('Priority',n)
        if priority != -1:
            content += str(tree.width(n)) + ',' + str(priority) + '\n'

    f = open('data/width_priority.csv','w')
    f.write(content)
    f.close()

#compare <parent, child> resolve time
def compare_resolution_time(tree):
    total_pair = 0
    weird_pairs = []
    nodes = tree.get_nodes_with_children()
    for n in nodes:
        p_time = query_db_field('Resolved',n)
        if p_time == -1:
            continue
        if p_time == '':
            #this bug is not resolved yet, assign a recent date
            p_time = '2014-12-30 00:00'
        parent_resolve_time = datetime.strptime(p_time, '%Y-%m-%d %H:%M')

        children = tree.get_children(n)
        for c in children:
            c_time = query_db_field('Resolved',c)
            if c_time == -1:
                continue
            if c_time == '':
                c_time = '2014-12-30 00:00'
            child_resolve_time = datetime.strptime(c_time, '%Y-%m-%d %H:%M')

            total_pair += 1
            if parent_resolve_time > child_resolve_time:
                weird_pairs.append(n+'-'+c)
    print('total pair: ' + str(total_pair))
    print('weird pair: ' + str(len(weird_pairs)))


def height_duration_correlation(tree):
    content = 'height,duration\n'
    nodes = tree.get_all_nodes()
    for n in nodes:
        rs = query_db_field('Reported,Resolved',n)
        if rs == -1:
            continue
        content += str(tree.height(n)) + ',' + str(get_duration(rs)) + '\n'

    f = open('data/height_duration.csv','w')
    f.write(content)
    f.close()

def width_duration_correlation(tree):
    content = 'width,duration\n'
    nodes = tree.get_all_nodes()
    for n in nodes:
        rs = query_db_field('Reported,Resolved',n)
        if rs == -1:
            continue
        content += str(tree.width(n)) + ',' + str(get_duration(rs)) + '\n'

    f = open('data/width_duration.csv','w')
    f.write(content)
    f.close()

#the resolution time of all bugs in DB
def get_resolution_time_list():
    block_bugs = []
    b_bugs = get_bugs('Block')
    print("# of bugs with block: " + str(len(b_bugs)))
    for b in b_bugs:
        rs = query_db_field('Reported,Resolved',b)
        block_bugs.append(get_duration(rs))
    print_stats('block_resolve_duration',block_bugs)

    depend_bugs = []
    d_bugs = get_bugs('Depend')
    print("# of bugs with depend: " + str(len(d_bugs)))
    for b in d_bugs:
        rs = query_db_field('Reported,Resolved',b)
        depend_bugs.append(get_duration(rs))
    print_stats('depend_resolve_duration',depend_bugs)

def get_bugs(type):
    #get all the bugs with type in db
    bugs_in_db = []
    cur = conn.cursor()
    cur.execute("select BugID from bug_report.Metadata where Mark" + type + '!=\'[]\'')
    for row in cur.fetchall():
        bugs_in_db.append(str(row[0]))
    cur.close()
    return bugs_in_db


def compare_root_leaf_attr(tree,attr):
    roots = tree.get_roots()
    root_attr = []
    leaves = tree.get_leaves()
    leaf_attr = []
    for bug in roots:
        rs = query_db_field(attr, bug)
        if rs != -1:
            root_attr.append(rs)
    for bug in leaves:
        rs = query_db_field(attr,bug)
        if rs != -1:
            leaf_attr.append(rs)
    print(Counter(root_attr).most_common())
    print(Counter(leaf_attr).most_common())


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

#the duration of mark dependency (between the bug reported time and the mark duration time)
def mark_depend_duration():
    block_duration = []
    depend_duration = []

    bugs = get_bugs_in_db()
    for b in bugs:
        #test
        # if not b == '537139':
        #     continue

        report_time = query_db_field('Reported',b)
        blocks = query_db_field('MarkBlock',b)
        depends = query_db_field('MarkDepend',b)

        block_time = []
        depend_time = []

        if blocks != '[]':
            blocks = blocks[1:len(blocks)-1]
            for bl in blocks.split(', '):
                bt = bl[1:len(bl)-1]
                block_time.append(bt)
        if depends != '[]':
            depends = depends[1:len(depends)-1]
            for de in depends.split(', '):
                dt = de[1:len(de)-1]
                depend_time.append(dt)

        for bt in block_time:
            duration = get_duration(report_time+'--'+bt)
            if duration >= 0:
                block_duration.append(duration)
            else:
                print(b + ' negative')
        for dt in depend_time:
            duration = get_duration(report_time+'--'+dt)
            if duration >=0 :
                depend_duration.append(duration)
            else:
                print(b + ' negative')

    print_stats('mark_block_duration',block_duration)
    print_stats('mark_depend_duration',depend_duration)


#convert from minutes to days, hours and minutes
def convert_time(m):
    days = math.floor(m/(60*24))
    hours = math.floor(m/60)-24 * days
    minutes = m - 60 * hours - 24 * 60 * days

    return str(days) + ' days ' + str(hours) + ' hours ' + str(minutes) + ' minutes'

#rs is begin_date--end_date
def get_duration(rs):
    reported = rs[:rs.find('--')]
    report_time = datetime.strptime(reported,'%Y-%m-%d %H:%M')
    resolved = rs[rs.find('--')+2:]
    if resolved == '':
        resolved = str(datetime.now())
        resolved = resolved[:resolved.rfind(':')]
    resolve_time = datetime.strptime(resolved,'%Y-%m-%d %H:%M')
    duration = (resolve_time - report_time).total_seconds() / 60 # in minutes
    return duration

def get_workaround():
    if os.path.isfile('data/has_workaround_bugs'):
        return [line.strip() for line in open('data/has_workaround_bugs')]
    else:
        return []


def workaround_impact(tree):
    workaround = get_workaround()
    nodes = tree.get_all_nodes()
    non_workaround = list(set(nodes)-set(workaround))

    wr_height = ''
    wr_width = ''
    non_wr_height = ''
    non_wr_width = ''

    for wr in workaround:
        wr_height += str(tree.height(wr))+','
        wr_width += str(tree.width(wr))+','
    for nwr in non_workaround:
        non_wr_height += str(tree.height(nwr))+','
        non_wr_width += str(tree.width(nwr))+','

    f = open('data/workaround_impact.csv','w')
    f.write(wr_height+'\n'+non_wr_height+'\n'+wr_width+'\n'+non_wr_width+'\n')
    f.close()


#print data to file
def write_data(file, data):
    content = ''
    for i in range(len(data)):
        if i == len(data)-1:
            content += str(data[i])
        else:
            content += str(data[i]) + ','
    f = open(file,'w')
    f.write(content)
    f.close()

#given a list, print stats
def print_stats(name, list):
    if 'duration' not in name:
        print("max "+name+": " + str(max(list)))
        print("min "+name+": " + str(min(list)))
        print("25% "+name+": " + str(np.percentile(list,25)))
        print("median " + name + ": " + str(np.percentile(list,50)))
        print("75% "+name+": " + str(np.percentile(list,75)))
        print("avg. "+name+": " + str(np.mean(list)))
        print("std. "+name+": " + str(np.std(list)))
    else:
        print("max "+name+": " + convert_time(max(list)))
        print("min "+name+": " + convert_time(min(list)))
        print("25% "+name+": " + convert_time(np.percentile(list,25)))
        print("median " + name + ": " + convert_time(np.percentile(list,50)))
        print("75% "+name+": " + convert_time(np.percentile(list,75)))
        print("avg. "+name+": " + convert_time(np.mean(list)))
        print("std. "+name+": " + convert_time(np.std(list)))

    print("===============")


if __name__ == "__main__":
    args = sys.argv
    host = args[1]
    port = args[2]
    user = args[3]
    pwd = args[4]
    conn = pymysql.connect(host=host, port=port, user=user, passwd=pwd, db='bug_report', charset='utf8')

    mark_depend_duration()
    #get_resolution_time_list()
    #mark_depend_duration()
    #tree = Tree()
    #workaround_impact(tree)
    #compare_root_leaf_attr(tree,'Platform')
    #height_priority_correlation(tree)
    # width_priority_correlation(tree)
    # height_duration_correlation(tree)
    # width_duration_correlation(tree)
    #compare_resolution_time(tree)
    # stats_roots_height(tree)
    # stats_children_width(tree)
    # stats_parents_width(tree)
    #priority(tree)
