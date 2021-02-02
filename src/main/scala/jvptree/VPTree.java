package jvptree;

import java.io.Serializable;
import java.util.*;

import jvptree.util.*;

/**
 * <p>A vantage-point tree (or vp-tree) is a binary space partitioning collection of points in a metric space. The main
 * feature of vantage point trees is that they allow for k-nearest-neighbor searches in any metric space in
 * <em>O(log(n))</em> time.</p>
 *
 * <p>Vantage point trees recursively partition points by choosing a &quot;vantage point&quot; and a distance threshold;
 * points are then partitioned into one collection that contains all of the points closer to the vantage point than the
 * chosen threshold and one collection that contains all of the points farther away than the chosen threshold.</p>
 *
 * <p>A {@linkplain DistanceFunction distance function} that satisfies the properties of a metric space must be provided
 * when constructing a vantage point tree. Callers may also specify a threshold selection strategy (a sampling median
 * strategy is used by default) and a node size to tune the ratio of nodes searched to points inspected per node.
 * Vantage point trees may be constructed with or without an initial collection of points, though specifying a
 * collection of points at construction time is the most efficient approach.</p>
 *
 * @author <a href="https://github.com/jchambers">Jon Chambers</a>
 *
 * @param P the base type of points between which distances can be measured
 * @param E the specific type of point contained in this vantage point tree
 */
public class VPTree<P, E extends P> implements SpatialIndex<P, E>, Serializable {

    private final DistanceFunction<P> distanceFunction;
    private final ThresholdSelectionStrategy<P, E> thresholdSelectionStrategy;
    private final int nodeCapacity;

    private VPTreeNode<P, E> rootNode;

    public static final int DEFAULT_NODE_CAPACITY = 32;

    /**
     * Constructs a new vp-tree that uses the given distance function and is initially empty. The constructed tree will
     * use a default {@link SamplingMedianDistanceThresholdSelectionStrategy} and node capacity
     * ({@value com.eatthepath.jvptree.VPTree#DEFAULT_NODE_CAPACITY} points).
     *
     * @param distanceFunction the distance function to use to calculate the distance between points
     */
    public VPTree(final DistanceFunction<P> distanceFunction) {
        this(distanceFunction, (Collection<E>) null);
    }

    /**
     * Constructs a new vp-tree that uses the given distance function and is initially populated with the given
     * collection of points. The constructed tree will use a default
     * {@link SamplingMedianDistanceThresholdSelectionStrategy} and node capacity
     * ({@value com.eatthepath.jvptree.VPTree#DEFAULT_NODE_CAPACITY} points).
     *
     * @param distanceFunction the distance function to use to calculate the distance between points
     * @param points the points with which this tree should be initially populated; may be {@code null}
     */
    public VPTree(final DistanceFunction<P> distanceFunction, final Collection<E> points) {
        this(distanceFunction, new SamplingMedianDistanceThresholdSelectionStrategy<P, E>(
                SamplingMedianDistanceThresholdSelectionStrategy.DEFAULT_NUMBER_OF_SAMPLES),
                VPTree.DEFAULT_NODE_CAPACITY, points);
    }

    /**
     * Constructs a new vp-tree that uses the given distance function and threshold selection strategy to partition
     * points. The tree will be initially empty and will have a default node capacity
     * ({@value com.eatthepath.jvptree.VPTree#DEFAULT_NODE_CAPACITY} points).
     *
     * @param distanceFunction the distance function to use to calculate the distance between points
     * @param thresholdSelectionStrategy the function to use to choose distance thresholds when partitioning nodes
     */
    public VPTree(final DistanceFunction<P> distanceFunction, final ThresholdSelectionStrategy<P, E> thresholdSelectionStrategy) {
        this(distanceFunction, thresholdSelectionStrategy, VPTree.DEFAULT_NODE_CAPACITY, null);
    }

    /**
     * Constructs a new vp-tree that uses the given distance function and threshold selection strategy to partition
     * points. The tree will be initially populated with the given collection of points and will have a default node
     * capacity ({@value com.eatthepath.jvptree.VPTree#DEFAULT_NODE_CAPACITY} points).
     *
     * @param distanceFunction the distance function to use to calculate the distance between points
     * @param thresholdSelectionStrategy the function to use to choose distance thresholds when partitioning nodes
     * @param points the points with which this tree should be initially populated; may be {@code null}
     */
    public VPTree(final DistanceFunction<P> distanceFunction, final ThresholdSelectionStrategy<P, E> thresholdSelectionStrategy, final Collection<E> points) {
        this(distanceFunction, thresholdSelectionStrategy, VPTree.DEFAULT_NODE_CAPACITY, points);
    }

    /**
     * Constructs a new vp-tree that uses the given distance function and threshold selection strategy to partition
     * points and is initially empty. The tree will attempt to partition nodes that contain more than
     * {@code nodeCapacity} points, and will be initially populated with the given collection of points.
     *
     * @param distanceFunction the distance function to use to calculate the distance between points
     * @param thresholdSelectionStrategy the function to use to choose distance thresholds when partitioning nodes
     * @param nodeCapacity the largest capacity a node may have before it should be partitioned
     */
    public VPTree(final DistanceFunction<P> distanceFunction, final ThresholdSelectionStrategy<P, E> thresholdSelectionStrategy, final int nodeCapacity) {
        this(distanceFunction, thresholdSelectionStrategy, nodeCapacity, null);
    }

    /**
     * Constructs a new vp-tree that uses the given distance function and threshold selection strategy to partition
     * points. The tree will attempt to partition nodes that contain more than {@code nodeCapacity} points, and will
     * be initially populated with the given collection of points.
     *
     * @param distanceFunction the distance function to use to calculate the distance between points
     * @param thresholdSelectionStrategy the function to use to choose distance thresholds when partitioning nodes
     * @param nodeCapacity the largest capacity a node may have before it should be partitioned
     * @param points the points with which this tree should be initially populated; may be {@code null}
     */
    public VPTree(final DistanceFunction<P> distanceFunction, final ThresholdSelectionStrategy<P, E> thresholdSelectionStrategy, final int nodeCapacity, final Collection<E> points) {
        this.distanceFunction = distanceFunction;
        this.thresholdSelectionStrategy = thresholdSelectionStrategy;
        this.nodeCapacity = nodeCapacity;

        if (points != null && !points.isEmpty()) {
            this.rootNode = new VPTreeNode<>(
                    points,
                    this.distanceFunction,
                    this.thresholdSelectionStrategy,
                    this.nodeCapacity);
        }
    }

    /*
     * (non-Javadoc)
     * @see com.eatthepath.jvptree.SpatialIndex#getNearestNeighbors(java.lang.Object, int)
     */
    @Override
    public List<E> getNearestNeighbors(final P queryPoint, final int maxResults) {
        final List<E> nearestNeighbors;

        if (this.rootNode == null) {
            nearestNeighbors = null;
        } else {
            final NearestNeighborCollector<P, E> collector =
                    new NearestNeighborCollector<>(queryPoint, this.distanceFunction, maxResults);

            this.rootNode.collectNearestNeighbors(collector);

            nearestNeighbors = collector.toSortedList();
        }

        return nearestNeighbors;
    }

    /*
     * (non-Javadoc)
     * @see com.eatthepath.jvptree.SpatialIndex#getAllWithinRange(java.lang.Object, double)
     */
    @Override
    public List<E> getAllWithinDistance(final P queryPoint, final double maxDistance) {
        final List<E> pointsWithinRange;

        if (this.rootNode == null) {
            pointsWithinRange = null;
        } else {
            pointsWithinRange = new ArrayList<>();
            this.rootNode.collectAllWithinDistance(queryPoint, maxDistance, pointsWithinRange);
        }

        return pointsWithinRange;
    }

    /*
     * (non-Javadoc)
     * @see java.util.Collection#size()
     */
    @Override
    public int size() {
        return this.rootNode == null ? 0 : this.rootNode.size();
    }

    /*
     * (non-Javadoc)
     * @see java.util.Collection#isEmpty()
     */
    @Override
    public boolean isEmpty() {
        return this.size() == 0;
    }

    /*
     * (non-Javadoc)
     * @see java.util.Collection#contains(java.lang.Object)
     */
    @Override
    @SuppressWarnings("unchecked")
    public boolean contains(final Object o) {
        try {
            return this.rootNode == null ? false : this.rootNode.contains((E) o);
        } catch (final ClassCastException e) {
            return false;
        }
    }

    /*
     * (non-Javadoc)
     * @see java.util.Collection#containsAll(java.util.Collection)
     */
    @Override
    public boolean containsAll(final Collection<?> points) {
        for (final Object point : points) {
            if (!this.contains(point)) { return false; }
        }

        return true;
    }

    /*
     * (non-Javadoc)
     * @see java.util.Collection#iterator()
     */
    @Override
    public Iterator<E> iterator() {
        final ArrayList<Iterator<E>> iterators = new ArrayList<>();

        if (this.rootNode != null) {
            this.rootNode.collectIterators(iterators);
        }

        return new MetaIterator<>(iterators);
    }

    /*
     * (non-Javadoc)
     * @see java.util.Collection#toArray()
     */
    @Override
    public Object[] toArray() {
        final Object[] array = new Object[this.size()];

        if (this.rootNode != null) {
            this.rootNode.addPointsToArray(array, 0);
        }

        return array;
    }

    /*
     * (non-Javadoc)
     * @see java.util.Collection#toArray(java.lang.Object[])
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T[] toArray(final T[] array) {
        final T[] arrayToPopulate;

        if (array.length < this.size()) {
            arrayToPopulate = (T[])java.lang.reflect.Array.newInstance(array.getClass().getComponentType(), this.size());
        } else {
            arrayToPopulate = array;
        }

        if (this.rootNode != null) {
            this.rootNode.addPointsToArray(arrayToPopulate, 0);
        }

        return arrayToPopulate;
    }

    /*
     * (non-Javadoc)
     * @see java.util.Collection#add(java.lang.Object)
     */
    @Override
    public boolean add(final E point) {
        return this.addAll(Collections.singletonList(point));
    }

    /*
     * (non-Javadoc)
     * @see java.util.Collection#addAll(java.util.Collection)
     */
    @SuppressWarnings("unchecked")
    public boolean addAll(final Collection<? extends E> points) {
        // Adding points always modifies a VPTree
        final boolean modified = !points.isEmpty();

        if (this.rootNode == null) {
            // We don't need to anneal here because annealing happens automatically as part of node construction
            this.rootNode = new VPTreeNode<>(
                    (Collection<E>) points,
                    this.distanceFunction,
                    this.thresholdSelectionStrategy,
                    this.nodeCapacity);
        } else {
            for (final E point : points) {
                this.rootNode.add(point);
            }

            if (modified) {
                this.rootNode.anneal();
            }
        }

        return modified;
    }

    /*
     * (non-Javadoc)
     * @see java.util.Collection#remove(java.lang.Object)
     */
    public boolean remove(final Object point) {
        return this.removeAll(Collections.singletonList(point));
    }

    /*
     * (non-Javadoc)
     * @see java.util.Collection#removeAll(java.util.Collection)
     */
    @SuppressWarnings("unchecked")
    public boolean removeAll(final Collection<?> points) {
        boolean pointRemoved = false;

        if (this.rootNode == null) {
            pointRemoved = false;
        } else {
            for (final Object point : points) {
                try {
                    pointRemoved = this.rootNode.remove((E) point) || pointRemoved;
                } catch (final ClassCastException ignored) {
                    // Ignored; no change to `pointRemoved`
                }
            }
        }

        if (pointRemoved) {
            this.rootNode.anneal();
        }

        return pointRemoved;
    }

    /*
     * (non-Javadoc)
     * @see java.util.Collection#retainAll(java.util.Collection)
     */
    @Override
    public boolean retainAll(final Collection<?> points) {
        final boolean modified = this.rootNode == null ? false : this.rootNode.retainAll(points);

        if (modified) {
            this.rootNode.anneal();
        }

        return modified;
    }

    /*
     * (non-Javadoc)
     * @see java.util.Collection#clear()
     */
    @Override
    public void clear() {
        this.rootNode = null;
    }


    //OUTLIER DETECTION CUSTOM FUNCTIONS

    private int minHeight(){
        return this.rootNode.minHeight();
    }

    private int maxHeight(){
        return this.rootNode.maxHeight();
    }

    public HashMap<String, Double> getThresholds(int height, String delimiter) {
        HashMap<String, Double> finalMap = new HashMap<>();
        int treeHeight = this.minHeight();
        if(treeHeight >= height){
            int tmp_height = 0;
            Queue<VPTreeNode> queue = new LinkedList<VPTreeNode>();
            queue.add(this.rootNode);
            do{
                Queue<VPTreeNode> tmp_queue = new LinkedList<VPTreeNode>();
                int count = 1;
                do{
                    VPTreeNode tmp = queue.poll();
                    Double threshold = tmp.getThresh();
                    String key = tmp_height + delimiter + count;
                    finalMap.put(key, threshold);
                    if(tmp_height<treeHeight) {
                        tmp_queue.add(tmp.get_closer());
                        tmp_queue.add(tmp.get_farther());
                    }
                    count++;
                }while (!queue.isEmpty());
                queue.clear();
                queue.addAll(tmp_queue);
                tmp_queue.clear();
                tmp_height+=1;
            }while (tmp_height <= height);
        }else{
            System.out.println("Height Error");
            System.exit(1);
        }
        return finalMap;
    }

    public HashMap<String, Object> getVPs(int height, String delimiter) {
        HashMap<String, Object> finalMap = new HashMap<>();
        int treeHeight = this.minHeight();
        if(treeHeight >= height){
            int tmp_height = 0;
            Queue<VPTreeNode> queue = new LinkedList<VPTreeNode>();
            queue.add(this.rootNode);
            do{
                Queue<VPTreeNode> tmp_queue = new LinkedList<VPTreeNode>();
                int count = 1;
                do{
                    VPTreeNode tmp = queue.poll();
                    Object vp = tmp.getVP();
                    String key = tmp_height + delimiter + count;
                    finalMap.put(key, vp);
                    if(tmp_height<treeHeight) {
                        tmp_queue.add(tmp.get_closer());
                        tmp_queue.add(tmp.get_farther());
                    }
                    count++;
                }while (!queue.isEmpty());
                queue.clear();
                queue.addAll(tmp_queue);
                tmp_queue.clear();
                tmp_height+=1;
            }while (tmp_height <= height);
        }else{
            System.out.println("Height Error");
            System.exit(1);
        }
        return finalMap;
    }


    //old
    public void createPartitions(Integer partitions){
        int height = 0;
        int temp = (int) Math.floor(Math.sqrt(partitions));
        int sqr1 = temp * temp;
        if(sqr1 == partitions) height = temp;
        else height = temp + 1;

        int treeHeight = this.minHeight();

        if(treeHeight > height){
            int tmp_height = 0;
            Queue<VPTreeNode> queue = new LinkedList<VPTreeNode>();
            queue.add(this.rootNode);
            do{
                Queue<VPTreeNode> tmp_queue = new LinkedList<VPTreeNode>();
                int count = 1;
                do{
                    VPTreeNode tmp = queue.poll();
                    tmp.setPartition(count);
                    if(tmp_height<treeHeight) {
                        tmp_queue.add(tmp.get_closer());
                        tmp_queue.add(tmp.get_farther());
                    }
                    count++;
                }while (!queue.isEmpty());
                queue.clear();
                queue.addAll(tmp_queue);
                tmp_queue.clear();
                tmp_height+=1;
            }while (tmp_height <= height);
        }else{
            System.out.println("Height Error");
        }
    }

    public List<String> findPartitions(final P queryPoint, final double maxDistance, int nopartitions) {
        final int maxHeight = (int) Math.sqrt(nopartitions);
        final List<String> partitions;

        if (this.rootNode == null) {
            partitions = null;
        } else {
            partitions = new ArrayList<>();
            this.rootNode.findPartitions(queryPoint, maxDistance, partitions, maxHeight, true);
        }

        return partitions;
    }

    //OUTLIER DETECTION END CUSTOM FUNCTIONS


}
