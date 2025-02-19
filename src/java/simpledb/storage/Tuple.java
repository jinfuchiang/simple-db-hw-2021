package simpledb.storage;

import java.io.Serializable;
import java.util.*;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    final private List<Field> fieldsData;

    private TupleDesc tupleDescription;

    private RecordId rid;
    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        tupleDescription = td;
        fieldsData = new ArrayList<>();
        rid = null;
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return tupleDescription;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        return rid;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        this.rid = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        if (fieldsData.size() > i)
            fieldsData.set(i, f);
        else {
            fieldsData.add(i, f);
        }
    }

    public int setField(int i, Tuple tuple) {
        int size = tuple.getTupleDesc().numFields();
        for (int j = 0; j < size; j++) {
            setField(i, tuple.getField(j));
            ++i;
        }
        return i;
    }
    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        return fieldsData.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        StringBuilder str = new StringBuilder();
        for (Field field:
             fieldsData) {
            str.append(field).append('\t');
        }
        str.deleteCharAt(str.length()-1);
        return str.toString();
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        return fieldsData.iterator();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        tupleDescription = td;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Tuple tuple = (Tuple) o;
        return fieldsData.equals(tuple.fieldsData) && tupleDescription.equals(tuple.tupleDescription);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldsData, tupleDescription);
    }
}
