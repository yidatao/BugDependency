from collections import namedtuple
import pymysql

class Tree:

    #build the tree
    def __init__(self, host, port, user, pwd):
        conn = pymysql.connect(host=host, port=port, user=user, passwd=pwd, db='bug_report', charset='utf8')
        #get all the bugs in db
        existing = []
        cur = conn.cursor()
        cur.execute("select BugID from bug_report.Metadata")
        for row in cur.fetchall():
            existing.append(str(row[0]))
        cur.close()

        tree_tuple = namedtuple('Tree','parents children')
        parents_map = {}
        children_map = {}

        cur = conn.cursor()
        cur.execute('select BugID,DependOn,Blocks from bug_report.Metadata')

        for row in cur.fetchall():
            node = str(row[0]) #BugID
            dependon = str(row[1])
            blocks = str(row[2])

            parents = []
            children = []

            if not dependon == '[]':
                dependon = dependon[1:len(dependon)-1]
                for d in dependon.split(', '):
                    d = d[1:len(d)-1]
                    #if the bug is in db TODO currently we cannot insert all related bugs to the database
                    if d in existing:
                        parents.append(d)
            if not blocks == '[]':
                blocks = blocks[1:len(blocks)-1]
                for b in blocks.split(', '):
                    b = b[1:len(b)-1]
                    #if the bug is in db TODO currently we cannot insert all related bugs to the database
                    if b in existing:
                        children.append(b)

            if len(parents) > 0:
                parents_map[node] = parents
                for p in parents:
                    if p in children_map:
                        children_map[p].append(node)
                    else:
                        children_map[p] = [node]
            if len(children) > 0:
                children_map[node] = children
                for c in children:
                    if c in parents_map:
                        parents_map[c].append(node)
                    else:
                        parents_map[c] = [node]

        cur.close()

        self.tree = tree_tuple(parents_map, children_map)

    #the tree is in fact a forest, so get all the roots
    def get_roots(self):
        roots = []
        for node in self.tree.children:
            #if a node has children but has no parent, then it's a root
            if node not in self.tree.parents:
                roots.append(node)
        return roots

    #get all the leaves
    def get_leaves(self):
        leaves = []
        for node in self.tree.parents:
            if node not in self.tree.children:
                leaves.append(node)
        return leaves

    #get the children of a node
    def get_children(self, node):
        return self.tree.children[node]

    #get the parents of a node
    def get_parents(self, node):
        return self.tree.parents[node]

    #the height of a node
    def height(self, node):
        #if node is the leaf
        if node not in self.tree.children:
            return 0
        else:
            children = self.tree.children[node]
            height_list = []
            for child in children:
                height_list.append(self.height(child))
            return max(height_list) + 1

    #the # of children of a node, refer to as width
    def width(self, node):
        if node not in self.tree.children:
            return 0
        else:
            return len(self.tree.children[node])


    #nodes that have parents
    def get_nodes_with_parents(self):
        return list(self.tree.parents.keys())

    #nodes that have children
    def get_nodes_with_children(self):
        return list(self.tree.children.keys())

    def get_all_nodes(self):
        return list(set(self.get_nodes_with_parents())|set(self.get_nodes_with_children()))