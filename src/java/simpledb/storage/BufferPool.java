package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    final static private PageNode head = new PageNode(null, false);

    public static class PageNode {
        Page page;
        PageNode prev;
        PageNode next;
        public PageNode(Page page, boolean isMoveToHead) {
            this.page = page;
            this.prev = this;
            this.next = this;
            if (isMoveToHead) moveToHead();
        }

        private void moveToHead() {
            out();
            pointToHead();
            modifyHead();
        }

        public void out() {
            prev.next = next;
            next.prev = prev;
            prev = null;
            next = null;
        }

        private void modifyHead() {
            head.next.prev = this;
            head.next = this;
        }

        private void pointToHead() {
            next = head.next;
            prev = head;
        }

        Page getPage(boolean isMoveToHead) {
            if (isMoveToHead)
                moveToHead();
            if (page == null) throw new RuntimeException("Should not access LRU head page.");
            return page;
        }
    }

    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private final int bufferPoolMaxSize;

    final private Map<PageId, PageNode> pageId2PageNode;


    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        bufferPoolMaxSize = numPages;
        pageId2PageNode = new HashMap<>(numPages);
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        PageNode pageNode = pageId2PageNode.get(pid);
        if (pageNode == null) {
            Page page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
            while (pageId2PageNode.size() >= bufferPoolMaxSize) {
                evictPage();
            }
            pageNode = new PageNode(page, true);
            pageId2PageNode.put(page.getId(), pageNode);
        }
        return pageNode.getPage(true);
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        List<Page> pages = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
        for (Page page:
             pages) {
            PageNode pageNode = pageId2PageNode.get(page.getId());
            if (pageNode == null) {
                while (pageId2PageNode.size() >= bufferPoolMaxSize) {
                    evictPage();
                }
                pageNode = new PageNode(page, true);
                pageId2PageNode.put(page.getId(), pageNode);
            }
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        PageId pageId = t.getRecordId().getPageId();
        Database.getCatalog().getDatabaseFile(pageId.getTableId()).deleteTuple(tid, t);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (PageId pageId:
            pageId2PageNode.keySet()) {
            flushPage(pageId);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        PageNode pageNode = pageId2PageNode.get(pid);
        pageNode.out();
        pageId2PageNode.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        PageNode pageNode = pageId2PageNode.get(pid);
        Page page = pageNode.getPage(false);
        if (page.isDirty() != null)
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        PageNode LRUNode = head.prev;
        Page LRUPage = LRUNode.getPage(false);
        PageId pid = LRUPage.getId();
        LRUNode.out();
        if (LRUPage.isDirty() != null) {
            try {
                HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(pid.getTableId());
                heapFile.writePage(LRUPage);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        pageId2PageNode.remove(LRUPage.getId());
    }

}
