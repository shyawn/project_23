package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.nio.Buffer;
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
	// introduced variables to store File file and TupleDesc tupleDesc
	private File file;
	private TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    	this.file = f;
    	this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
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
        // some code goes here
        return file.getAbsoluteFile().hashCode();
        //throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid){
        // some code goes here
        // return Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            // long pos = pid.getPageNumber() * (long) BufferPool.getPageSize();
            int offset = BufferPool.getPageSize() * pid.getPageNumber();
            byte[] data = new byte[BufferPool.getPageSize()];
            if (offset + BufferPool.getPageSize() > this.file.length()) {
                System.err.println("Page offset exceeds max size, error!");
                System.exit(1);
            }
            raf.seek(offset);
            raf.readFully(data);
            raf.close();

            return new HeapPage((HeapPageId) pid, data);
        } catch (Exception e) {}
        return null;
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
        // some code goes here
        // return tupleDesc.numFields();
        return (int) this.file.length() / BufferPool.getPageSize();

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

    private class HeapFileIterator implements DbFileIterator {
        
        // private static final long serialVersionUID = 1L;
        private int curPage = 0;
        private Iterator<Tuple> curItr = null;
        private TransactionId tid;
        private boolean open = false;;
        
        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
        }
        
        @Override
        public void open() throws DbException, TransactionAbortedException {
            open = true;
            curPage = 0;
            if (curPage >= numPages()) {
                return;
            }
            curItr = ((HeapPage) Database.getBufferPool().getPage(tid,
                    new HeapPageId(getId(), curPage), Permissions.READ_ONLY))
                    .iterator();
            advance();
        }

        private void advance() throws DbException, TransactionAbortedException {
            while (!curItr.hasNext()) {
                curPage++;
                if (curPage < numPages()) {
                    curItr = ((HeapPage) Database.getBufferPool().getPage(tid,
                            new HeapPageId(getId(), curPage),
                            Permissions.READ_ONLY)).iterator();
                } else {
                    break;
                }
            }
        }

        @Override
        public boolean hasNext() throws DbException,
                TransactionAbortedException {
            if (!open) {
                return false;
            }
            return curPage < numPages();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException,
                NoSuchElementException {
            if (!open) {
                throw new NoSuchElementException("iterator not open.");
            }
            if (!hasNext()) {
                throw new NoSuchElementException("No more tuples.");
            }
            Tuple result = curItr.next();
            advance();
            return result;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            if (!open) {
                throw new DbException("iterator not open yet.");
            }
            close();
            open();          
        }

        @Override
        public void close() {
            curItr = null;
            curPage = 0;
            open = false;
        }

    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        // int tableid = Database.getCatalog().getTableId(file.getName());
        // PageId pid = Database.getCatalog().getDatabaseFile(tableid).get;
        // return Database.getBufferPool().getPage(tid, pid, null);
        // return null;
        return new HeapFileIterator(tid);
    }

}

