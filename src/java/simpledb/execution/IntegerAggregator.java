package simpledb.execution;

import simpledb.common.Catalog;
import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int groupByFieldIndex;
    private Type groupByType;
    private int aggregateFieldIndex;
    private Op op;
    private Map<Field, List<Field>> group2Aggregate;
    static private final IntField dummyField = new IntField(0);
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.groupByFieldIndex = gbfield;
        this.groupByType = gbfieldtype;
        this.aggregateFieldIndex = afield;
        this.op = what;
        group2Aggregate = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupField;
        if (groupByFieldIndex == NO_GROUPING) {
            groupField = dummyField;
        } else {
            groupField = tup.getField(groupByFieldIndex);
        }
        Field aggregateField = tup.getField(aggregateFieldIndex);
        List<Field> aggregateList = group2Aggregate.get(groupField);

        if (aggregateList != null) {
            aggregateList.add(aggregateField);
        }
        else {
            aggregateList = new ArrayList<>();
            aggregateList.add(aggregateField);
            group2Aggregate.put(groupField, aggregateList);
        }
    }

    public Field aggregate(List<Field> fields) {
        int ret = 0;
        switch (op) {
            case MIN:
                int mi = ((IntField)fields.get(0)).getValue();
                for (Field field:
                     fields) {
                    IntField iField = (IntField) field;
                    if (mi > iField.getValue()) mi = iField.getValue();
                }
                return new IntField(mi);
            case MAX:
                int ma = ((IntField)fields.get(0)).getValue();
                for (Field field:
                        fields) {
                    IntField iField = (IntField) field;
                    if (ma < iField.getValue()) ma = iField.getValue();
                }
                return new IntField(ma);
            case AVG:
            case SUM:
                int sum = 0;
                for (Field field:
                        fields) {
                    IntField iField = (IntField) field;
                    sum += iField.getValue();
                }
                if(op == Op.AVG)
                    sum = sum / fields.size();
                return new IntField(sum);
            case COUNT:
                return new IntField(fields.size());
            default:
                throw new UnsupportedOperationException("Unimplemented");
        }
    }
    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggergateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        List<Tuple> tuples = new ArrayList<>();
        Type[] fieldsType;
        TupleDesc resultTupleDescription;
        if (groupByFieldIndex == NO_GROUPING) {
            fieldsType = new Type[]{Type.INT_TYPE};
            resultTupleDescription = new TupleDesc(fieldsType);
            Tuple tuple = new Tuple(resultTupleDescription);

            tuple.setField(0, aggregate(group2Aggregate.get(dummyField)));
            tuples.add(tuple);
        } else {
            fieldsType = new Type[]{group2Aggregate.keySet().iterator().next().getType(), Type.INT_TYPE};
            resultTupleDescription = new TupleDesc(fieldsType);
            for (Map.Entry<Field, List<Field>> entry:
                    group2Aggregate.entrySet()) {
                Tuple tuple = new Tuple(resultTupleDescription);
                tuple.setField(0, entry.getKey());
                tuple.setField(1, aggregate(entry.getValue()));
                tuples.add(tuple);
            }
        }
        return new TupleIterator(resultTupleDescription, tuples);
    }

    public TupleDesc getTupleDesc(TupleDesc tupleDesc) {
        String aFieldName = String.format("%s (%s)", op.toString(), tupleDesc.getFieldName(aggregateFieldIndex));
        String groupFieldName = tupleDesc.getFieldName(groupByFieldIndex);
        if(groupByFieldIndex == NO_GROUPING) {
            Type[] fieldsType = new Type[]{Type.INT_TYPE};
            return new TupleDesc(fieldsType, new String[]{aFieldName});
        } else {
            Type[] fieldsType = new Type[]{tupleDesc.getFieldType(groupByFieldIndex), Type.INT_TYPE};
            return new TupleDesc(fieldsType, new String[]{aFieldName, groupFieldName});
        }
    }

    @Override
    public void clear() {
        group2Aggregate.clear();
    }
}
