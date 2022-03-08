package simpledb.execution;

import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator childOpIterator;
    final private Predicate predicate;
    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        predicate = p;
        childOpIterator = child;
    }

    public Predicate getPredicate() {
        return predicate;
    }

    public TupleDesc getTupleDesc() {
        return childOpIterator.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        super.open();
        childOpIterator.open();
    }

    public void close() {
        super.close();
        childOpIterator.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        while(childOpIterator.hasNext()) {
            Tuple tuple = childOpIterator.next();
            if(predicate.filter(tuple)) return tuple;
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{childOpIterator};
    }

    /**
     *
     * @param children
     *            children.length should be 1
     *            if not, ignore children[1:]
     */
    @Override
    public void setChildren(OpIterator[] children) {
        if (children.length == 1) {
            childOpIterator = children[0];
        }
    }

}
