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
    	return this.file.getAbsoluteFile().hashCode();
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
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            int offset = BufferPool.getPageSize() * pid.getPageNumber();
            byte[] data = new byte[BufferPool.getPageSize()];
            
            // if the offset exceeds the file length
            if (offset < 0 || offset >= this.file.length()) {
                throw new Exception("Page does not exist");
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
    	return (int) Math.ceil(this.file.length() / BufferPool.getPageSize());
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
    	
    	private int currentPage;
    	private Iterator<Tuple> heapPageIterator;
    	private TransactionId tid;
    	private boolean isOpen = false;
    	
    	public HeapFileIterator(TransactionId tid) {
    		this.tid = tid;
    	}
    	
    	/**
         * Opens the iterator
         * @throws DbException when there are problems opening/accessing the database.
         */
    	@Override
    	public void open() throws DbException, TransactionAbortedException {
    		isOpen = true;
    		currentPage = 0;
    		
    		if (currentPage >= numPages()) {
    			return;
    		}
    		
    		heapPageIterator = ((HeapPage) Database.getBufferPool().getPage(tid,
    				new HeapPageId(getId(), currentPage), Permissions.READ_ONLY)).iterator();
    		
    		// if current page has no more tuples, change heapPageIterator to next page's iterator
    		if (!heapPageIterator.hasNext()) {
    			UseNextPageIterator();
    		}
    		
    	}
    	
    	// function to help get the next page's iterator
    	private void UseNextPageIterator() throws DbException, TransactionAbortedException {
    		// if current headPageIterator has reached the end of the page
    		while (!heapPageIterator.hasNext()) {
    			// proceed to the next page
    			currentPage += 1;
    			
    			// if the new currentPage number is valid
    			if (currentPage < numPages()) {
    				// update heapPageIterator with the new iterator for the new currentPage
    				heapPageIterator = ((HeapPage) Database.getBufferPool().getPage(tid,
    						new HeapPageId(getId(), currentPage),
    						Permissions.READ_ONLY)).iterator();
    			} else {
    				break;
    			}
    		}
    	}
    	
    	/** @return true if there are more tuples available, false if no more tuples or iterator isn't open. */
    	@Override
    	public boolean hasNext() throws DbException, TransactionAbortedException {
    		if (!isOpen) {
    			return false;
    		}
    		
    		return currentPage < numPages();
    	}
    	
    	/**
         * Gets the next tuple from the operator (typically implementing by reading
         * from a child operator or an access method).
         *
         * @return The next tuple in the iterator.
         * @throws NoSuchElementException if there are no more tuples
         */
    	@Override
    	public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
    		if (!isOpen) {
    			throw new NoSuchElementException("Tterator is not open.");
    		}
    		
    		if (!hasNext()) {
    			throw new NoSuchElementException("No more tuples.");
    		}
    		
    		Tuple result = heapPageIterator.next();
    		
    		// if current page has no more tuples, change heapPageIterator to next page's iterator
    		if (!heapPageIterator.hasNext()) {
    			UseNextPageIterator();
    		}
    		
    		return result;
    	}
    	
    	/**
         * Resets the iterator to the start.
         * @throws DbException When rewind is unsupported.
         */
    	@Override
    	public void rewind() throws DbException, TransactionAbortedException {
    		if (!isOpen) {
    			throw new DbException("Iterator is not yet open.");
    		}
    		
    		close();
    		open();          
    	}

    	/**
         * Closes the iterator.
         */
    	@Override
    	public void close() {
    		heapPageIterator = null;
    		currentPage = 0;
    		isOpen = false;
    	}
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
    	return new HeapFileIterator(tid);
    }

}

