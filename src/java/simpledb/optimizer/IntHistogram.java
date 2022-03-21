package simpledb.optimizer;

import simpledb.execution.Predicate;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int min;
    private int max;
    private int[] histogram;
    private int delta;
    private int nTuples;
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        this.max = max;
        this.min = min;
        histogram = new int[buckets];
        delta = ceilDivide(max-min+1, Math.min(buckets, max - min + 1));
    }

    public int ceilDivide(int i, int j) {
        return (i - 1) / j + 1;
    }

    public int whichBuckets(int v) {
        return (v - min) / delta;
    }

    public int getValue(int v) {
        return histogram[whichBuckets(v)];
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        nTuples++;
        histogram[whichBuckets(v)]++;
    }

    public double estimateEqualSelectivity(int v) {
        if (v < min || v > max) return 0;
        int h = getValue(v);
        return (double) ceilDivide(h, delta) / nTuples;
    }

    public double estimateGreaterThanSelectivity(int v, boolean isClosed) {
        int i = whichBuckets(v);
        int numTargetTuples = 0;
        if (i >= 0 && i < histogram.length) {
            int bRight = (i + 1) * delta + min;
            int h = histogram[i];
            int w = bRight - v;
            if (!isClosed) {
                --w;
            }
            numTargetTuples = w * h;
        }

        if (i < 0) {
            i = -1;
        }

        for (int j = i+1; j < histogram.length; j++) {
            numTargetTuples += histogram[j];
        }

        return (double) numTargetTuples / nTuples;
    }
    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        switch (op) {
            case EQUALS:
            case LIKE:
                return estimateEqualSelectivity(v);
            case NOT_EQUALS:
                return 1 - estimateEqualSelectivity(v);
            case GREATER_THAN:
                return estimateGreaterThanSelectivity(v, false);
            case GREATER_THAN_OR_EQ:
                return estimateGreaterThanSelectivity(v, true);
            case LESS_THAN:
                return 1 - estimateGreaterThanSelectivity(v, true);
            case LESS_THAN_OR_EQ:
                return 1 - estimateGreaterThanSelectivity(v, false);
            default:
                return 0;
        }
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
