package simpledb.storage;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {
	
	// introduced attributes to store PageId and tupleno
	private PageId pageId;
	private int tupleNumber;

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        // some code goes here
    	this.pageId = pid;
    	this.tupleNumber = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int getTupleNumber() {
        // some code goes here
        return this.tupleNumber;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        // some code goes here
        return this.pageId;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        // some code goes here
    	boolean result = true;
    	
    	if (o instanceof RecordId) {
    		RecordId testObject = (RecordId) o;
    		
    		// they are not equal if they do not have the same hashcode
    		if (testObject.hashCode() != this.hashCode()) {
    			result = false;
    		}
    		
    	} else {
    		result = false;
    	}
    	
    	return result;
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        // some code goes here
    	int pageIdHashCode = this.pageId.hashCode();
    	int tupleNumberHashCode = Integer.toString(this.tupleNumber).hashCode();
    	int recordIdHashCode = Integer.parseInt(Integer.toString(pageIdHashCode) + Integer.toString(tupleNumberHashCode));
    	
    	return recordIdHashCode;
    }

}
