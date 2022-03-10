package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int groupFieldIndex;
    private int aggregateFieldIndex;
    private Op op;
    private Map<Field, Integer> group2Cnt;
    static private final IntField dummyField = new IntField(0);

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        groupFieldIndex = gbfield;
        aggregateFieldIndex = afield;
        group2Cnt = new HashMap<>();
        if (!what.equals(Op.COUNT)) throw new IllegalArgumentException(what.toString());
        op = Op.COUNT;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupField = tup.getField(groupFieldIndex);
        Field aggregateField = tup.getField(aggregateFieldIndex);
        if (groupFieldIndex == NO_GROUPING) {
            groupField = dummyField;
        }
        group2Cnt.merge(groupField, 1, Integer::sum);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        List<Tuple> tuples = new ArrayList<>();
        Type[] fieldsType;
        TupleDesc resultTupleDescription;
        if (groupFieldIndex == NO_GROUPING) {
            fieldsType = new Type[]{Type.INT_TYPE};
            resultTupleDescription = new TupleDesc(fieldsType);
            Tuple tuple = new Tuple(resultTupleDescription);
            tuple.setField(0, new IntField(group2Cnt.get(dummyField)));
            tuples.add(tuple);
        } else {
            fieldsType = new Type[]{group2Cnt.keySet().iterator().next().getType(), Type.INT_TYPE};
            resultTupleDescription = new TupleDesc(fieldsType);
            for (Map.Entry<Field, Integer> entry:
                    group2Cnt.entrySet()) {
                Tuple tuple = new Tuple(resultTupleDescription);
                tuple.setField(0, entry.getKey());
                tuple.setField(1, new IntField(entry.getValue()));
                tuples.add(tuple);
            }
        }
        return new TupleIterator(resultTupleDescription, tuples);
    }

    @Override
    public TupleDesc getTupleDesc(TupleDesc tupleDesc) {
        String aFieldName = String.format("%s (%s)", op.toString(), tupleDesc.getFieldName(aggregateFieldIndex));
        String groupFieldName = tupleDesc.getFieldName(groupFieldIndex);
        if(groupFieldIndex == NO_GROUPING) {
            Type[] fieldsType = new Type[]{Type.INT_TYPE};
            return new TupleDesc(fieldsType, new String[]{aFieldName});
        } else {
            Type[] fieldsType = new Type[]{tupleDesc.getFieldType(groupFieldIndex), Type.INT_TYPE};
            return new TupleDesc(fieldsType, new String[]{aFieldName, groupFieldName});
        }
    }

    @Override
    public void clear() {
        group2Cnt.clear();
    }

}
