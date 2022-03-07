package simpledb.storage;

import simpledb.common.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc tupleDescription;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.tupleDescription = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return tupleDescription;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        HeapPageId heapPageId = (HeapPageId) pid;
        int pageSize = BufferPool.getPageSize();

        try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file))) {
            byte[] pageBuf = new byte[BufferPool.getPageSize()];
            long offset = (long) (heapPageId.getPageNumber()) * pageSize;
            // skip header and the first pageNo pages
            if (bis.skip(offset) != offset) {
                throw new IllegalArgumentException(
                    "Unable to seek to correct place in HeapFile");
            }
            int retval = bis.read(pageBuf, 0, pageSize);
            if (retval == -1) {
                throw new IllegalArgumentException("Read past end of table");
            }
            if (retval < BufferPool.getPageSize()) {
                throw new IllegalArgumentException("Unable to read "
                        + pageSize + " bytes from HeapFile");
            }
            Debug.log(1, "Heapfile.readPage: read page %d", heapPageId.getPageNumber());
            return new HeapPage(heapPageId, pageBuf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) file.length() / BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this, tid);
    }

}

 class HeapFileIterator extends AbstractDbFileIterator {

    final TransactionId transactionId;
    final HeapFile heapFile;
    Iterator<Tuple> it;
    int nextPageNo;

    public HeapFileIterator(HeapFile heapFile, TransactionId tid) {
        this.heapFile = heapFile;
        this.transactionId = tid;
        nextPageNo = heapFile.numPages();
        it = null;
    }

    @Override
    protected Tuple readNext() throws DbException, TransactionAbortedException {
        if (it != null && !it.hasNext())
            it = null;

        while ((it == null || !it.hasNext()) && nextPageNo < heapFile.numPages()) {
            HeapPageId pageId = new HeapPageId(heapFile.getId(), nextPageNo++);
            BufferPool bufferPool = Database.getBufferPool();
            HeapPage heapPage = (HeapPage) bufferPool.getPage(transactionId, pageId, Permissions.READ_ONLY);
            it = heapPage.iterator();
        }

        if (it != null && it.hasNext()) return it.next();
        return null;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        nextPageNo = 0;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    @Override
    public void close() {
        super.close();
        it = null;
        nextPageNo = heapFile.numPages();
    }
 }

