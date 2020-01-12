package mtree.utils;

import java.lang.Comparable;
import java.lang.IllegalArgumentException;
import java.lang.UnsupportedOperationException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class FibonacciHeap<T extends Comparable<T>> {

    private Node<T> minNode;
    public int size;

    public FibonacciHeap() {
        minNode = null;
        size = 0;
    }

    private FibonacciHeap(Node<T> node) {
        minNode = node;
        size = 1;
    }

    private FibonacciHeap(Node<T> minNode, int size) {
        this.minNode = minNode;
        this.size = size;
    }

    public boolean isEmpty() {
        return minNode == null;
    }

    public void clear() {
        minNode = null;
        size = 0;
    }

    public Node<T> insert(T key) {
        Node<T> node = new Node<T>(key);
        minNode = mergeLists(minNode, node);
        size++;
        return node;
    }

    public Node<T> findMinimum() {
        return minNode;
    }

    // luan add
    public void increaseKey(Node<T> node, T newKey) {
        if (newKey.compareTo(node.key) < 0) {
            throw new IllegalArgumentException(
                    "New key is smaller than old key.");
        }

        node.key = newKey;
//        

        Node<T> parent = node.parent;
        Node<T> child = node.child;

        if(node.parent!=null)
        node.parent.child = findMinNodeInList(node);
        if (child != null && newKey.compareTo(child.key) > 0) {
            //?
            switchParentVsChild(node, child);

            
           
            if (minNode == node) {
                minNode = child;
                findMinNode();
            }
        }
        else{
             if (minNode == node) {
               
                findMinNode();
            }
        }

//        
    }

    public void switchParentVsChild(Node<T> parent, Node<T> child) {
        if (child == null) {
            return;
        }
        if (parent == null) {
            return;
        }
        child.parent = parent.parent;
        Node<T> temp = child.next;
        while (temp != child) {
            temp.parent = child;
            temp = temp.next;

        }
        if(child.child!=null){
            child.child.parent= parent;
            temp = child.child.next;
            while(temp!=child.child){
                temp.parent = parent;
                temp = temp.next;
            }
        }
        int t2 = child.degree;
        child.degree = parent.degree;
        parent.degree = t2;
        child.isMarked = parent.isMarked;
        parent.child = child.child;
//        child.child = parent;

        Node<T> nextOfChild = child.next;
        Node<T> prevOfChild = child.prev;
        if (parent.next != parent && child.next != child) {
            child.next = parent.next;
            parent.next.prev = child;
            child.prev = parent.prev;
            parent.prev.next = child;
            parent.next = nextOfChild;
            nextOfChild.prev = parent;
            parent.prev = prevOfChild;
            prevOfChild.next = parent;
        } else if (parent.next != parent && nextOfChild == child) {
            child.next = parent.next;
            parent.next.prev = child;
            child.prev = parent.prev;
            parent.prev.next = child;
            parent.next = parent;
            parent.prev = parent;
        } else if (parent.next == parent && nextOfChild != child) {
            parent.next = nextOfChild;
            nextOfChild.prev = parent;
            parent.prev = prevOfChild;
            prevOfChild.next = parent;
            child.next = child;
            child.prev = child;
        }

        parent.parent = child;
        /**
         * update min node in list of parent and child
         *
         */
//        if (parent.parent != null) {
//            parent.parent.child = findMinNodeInList(parent);
//        }
        if (child.parent != null) {
            child.parent.child = findMinNodeInList(child);
        }
        
        child.child = findMinNodeInList(parent);

        if (parent.child != null && parent.key.compareTo(parent.child.key) > 0) {
            switchParentVsChild(parent, parent.child);
        }

    }

    public Node<T> findMinNodeInList(Node<T> node) {
        Node<T> result = node;
        Node<T> temp = node.next;
        while (temp != node) {

            if (temp.key.compareTo(result.key) < 0) {
                result = temp;
            }
            temp = temp.next;
        }
        return result;
    }

    public void findMinNode() {

        Node<T> node = minNode;
        Node<T> min = minNode;
        while (node.next != minNode) {
            node = node.next;
            if (node.key.compareTo(min.key) < 0) {
                min = node;
            }

        }

        minNode = min;

    }

    public void decreaseKey(Node<T> node, T newKey) {
        if (newKey.compareTo(node.key) > 0) {
            throw new IllegalArgumentException("New key is larger than old key.");
        }

        node.key = newKey;
        Node<T> parent = node.parent;
        if (parent != null && node.compareTo(parent) < 0) {
            cut(node, parent);
            cascadingCut(parent);
        }
        if (node.compareTo(minNode) < 0) {
            minNode = node;
        }
    }

    private void cut(Node<T> node, Node<T> parent) {
        
        
        parent.child = findMinNodeInList(node);
        removeNodeFromList(node);
        parent.degree--;
        if (parent.degree < 0) {
            parent.degree = 0;
        }
        mergeLists(minNode, node);

        node.isMarked = false;
    }

    //luan add
    private void cutButNotRemoveFromList(Node<T> node, Node<T> parent) {
//       / removeNodeFromList(node);
        parent.degree--;
        if (parent.degree < 0) {
            parent.degree = 0;
        }
        mergeLists(minNode, node);

        node.isMarked = false;
    }

    private void cascadingCut(Node<T> node) {
        Node<T> parent = node.parent;
        if (parent != null) {
            if (node.isMarked) {
                cut(node, parent);
                cascadingCut(parent);
            } else {
                node.isMarked = true;
            }
        }
    }

    public void delete(Node<T> node) {
        // This is a special implementation of decreaseKey that sets the
        // argument to the minimum value. This is necessary to make generic keys
        // work, since there is no MIN_VALUE constant for generic types.
        node.isMinimum = true;
        Node<T> parent = node.parent;
        if (parent != null) {
            cut(node, parent);
            cascadingCut(parent);
        }
        minNode = node;

        extractMin();
    }

    public Node<T> extractMin() {
        Node<T> extractedMin = minNode;
        if (extractedMin != null) {
            // Set parent to null for the minimum's children
            if (extractedMin.child != null) {
                Node<T> child = extractedMin.child;
                do {
                    child.parent = null;
                    child = child.next;
                } while (child != extractedMin.child);
            }
//            extractedMin.child = null;

            Node<T> nextInRootList = minNode.next == minNode ? null : minNode.next;

            // Remove min from root list
            removeNodeFromList(extractedMin);
            size--;

            // Merge the children of the minimum node with the root list
            minNode = mergeLists(nextInRootList, extractedMin.child);

            if (nextInRootList != null) {
                minNode = nextInRootList;
                consolidate();
            }
        }
        extractedMin.parent= null;
        extractedMin.child = null;
        return extractedMin;
    }

    private void consolidate() {
        List<Node<T>> aux = new ArrayList<Node<T>>();
        NodeListIterator<T> it = new NodeListIterator<T>(minNode);
        while (it.hasNext()) {
            Node<T> current = it.next();

            while (aux.size() <= current.degree + 1) {
                aux.add(null);
            }

            // If there exists another node with the same degree, merge them
            while (aux.get(current.degree) != null) {
                if (current.key.compareTo(aux.get(current.degree).key) > 0) {
                    Node<T> temp = current;
                    current = aux.get(current.degree);
                    aux.set(current.degree, temp);
                }
                linkHeaps(aux.get(current.degree), current);
                aux.set(current.degree, null);
                current.degree++;
            }

            while (aux.size() <= current.degree + 1) {
                aux.add(null);
            }
            aux.set(current.degree, current);
        }

        minNode = null;
        for (int i = 0; i < aux.size(); i++) {
            if (aux.get(i) != null) {
                // Remove siblings before merging
                aux.get(i).next = aux.get(i);
                aux.get(i).prev = aux.get(i);
                minNode = mergeLists(minNode, aux.get(i));
            }
        }
    }

    private void removeNodeFromList(Node<T> node) {
        Node<T> prev = node.prev;
        Node<T> next = node.next;
        prev.next = next;
        next.prev = prev;

        node.next = node;
        node.prev = node;
    }

    private void linkHeaps(Node<T> max, Node<T> min) {
        removeNodeFromList(max);
        min.child = mergeLists(max, min.child);
        max.parent = min;
        max.isMarked = false;
    }

    // Union another fibonacci heap with this one
    public void union(FibonacciHeap<T> other) {
        minNode = mergeLists(minNode, other.minNode);
        size += other.size;
    }

    // Merges two lists and returns the minimum node
    public static <T extends Comparable<T>> Node<T> mergeLists(Node<T> a, Node<T> b) {

        if (a == null && b == null) {
            return null;
        }
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }

        Node<T> temp = a.next;
        a.next = b.next;
        a.next.prev = a;
        b.next = temp;
        b.next.prev = b;

        return a.compareTo(b) < 0 ? a : b;
    }

    public void print() {
        System.out.println("Fibonacci heap:");
        if (minNode != null) {
            minNode.print(0);
        }
    }

    public static class Node<T extends Comparable<T>> implements Comparable<Node<T>> {

        private T key;
        private int degree;
        public Node<T> parent;
        public Node<T> child;
        public Node<T> prev;
        public Node<T> next;
        private boolean isMarked;
        private boolean isMinimum;

        public Node() {
            key = null;
        }

        public Node(T key) {
            this.key = key;
            next = this;
            prev = this;
        }

        public T getKey() {
            return key;
        }

        public int compareTo(Node<T> other) {
            return this.key.compareTo(other.key);
        }

        private void print(int level) {
            Node<T> curr = this;
            do {
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < level; i++) {
                    sb.append(" ");
                }
                sb.append(curr.key.toString());
                System.out.println(sb.toString());
                if (curr.child != null) {
                    curr.child.print(level + 1);
                }
                curr = curr.next;
            } while (curr != this);
        }
    }

    // This Iterator is used to simplify the consolidate() method. It works by
    // gathering a list of the nodes in the list in the constructor since the
    // nodes can change during consolidation.
    public static class NodeListIterator<T extends Comparable<T>> implements Iterator<Node<T>> {

        private Queue<Node<T>> items = new LinkedList<Node<T>>();

        public NodeListIterator(Node<T> start) {
            if (start == null) {
                return;
            }

            Node<T> current = start;
            do {
                items.add(current);
                current = current.next;
            } while (start != current);
        }

        public boolean hasNext() {
            return items.peek() != null;
        }

        public Node<T> next() {
            return items.poll();
        }

        public void remove() {
            throw new UnsupportedOperationException("NodeListIterator.remove is not implemented");
        }
    }

}
