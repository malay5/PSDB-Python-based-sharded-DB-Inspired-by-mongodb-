class BPlusTree:
    def __init__(self, order=3):
        self.order = order
        self.root = BPlusTreeNode(is_leaf=True)
    
    def insert(self, value, doc_id):
        node = self._find_leaf(value)
        node.insert(value, doc_id)
        
        if len(node.keys) > self.order:
            self._split_node(node)

    def _find_leaf(self, value):
        """Find the leaf node where the value should go."""
        node = self.root
        while not node.is_leaf:
            node = node.get_child(value)
        return node

    def _split_node(self, node):
        """Handle the splitting of a full node."""
        middle_key = node.keys[len(node.keys) // 2]
        new_node = BPlusTreeNode(is_leaf=node.is_leaf)
        node.split(new_node)

        # If it's the root node, create a new root
        if node == self.root:
            new_root = BPlusTreeNode(is_leaf=False)
            new_root.keys.append(middle_key)
            new_root.children = [node, new_node]
            self.root = new_root
        else:
            parent_node = node.parent
            parent_node.insert(middle_key, new_node)

    def search(self, op, value):
        """Search for a value in the B+ Tree."""
        node = self._find_leaf(value)
        return node.search(op, value)
    
    def delete(self, value, doc_id):
        node = self._find_leaf(value)
        node.delete(value, doc_id)


    
class BPlusTreeNode:
    def __init__(self, is_leaf=False):
        self.is_leaf = is_leaf
        self.keys = []
        self.children = []

    def insert(self, value, doc_id):
        """Insert key-value pair (value, doc_id) into this node."""
        self.keys.append((value, doc_id))
        self.keys.sort()  # Ensure keys are sorted

    def search(self, op, value):
        """Search for values in the node."""
        return [doc_id for val, doc_id in self.keys if self._compare(op, val, value)]

    def _compare(self, op, key, value):
        """Compare keys using the operation provided (e.g., equals, greater than, etc.)."""
        if op == 'eq':
            return key == value
        elif op == 'gt':
            return key > value
        elif op == 'lt':
            return key < value
        return False
    
    def delete(self, value, doc_id):
        self.keys = [(v, d) for v, d in self.keys if not (v == value and d == doc_id)]



class IndexManager:
    def __init__(self):
        self.indexes = {}  # field -> BPlusTree

    def create_index(self, field):
        """Creates an index for a given field."""
        if field not in self.indexes:
            self.indexes[field] = BPlusTree()

    def insert(self, field, value, doc_id):
        if field in self.indexes:
            self.indexes[field].insert(value, doc_id)

    def get_index(self, field):
        """Returns the index for a field."""
        return self.indexes.get(field)

    def query(self, field, op, value):
        """Performs a query on the index."""
        index = self.get_index(field)
        if not index:
            return []
        return index.search(op, value)  # Custom method in your B+ Tree

