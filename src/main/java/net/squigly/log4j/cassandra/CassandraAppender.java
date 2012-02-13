package net.squigly.log4j.cassandra;

import java.util.ArrayList;
import java.util.Iterator;

import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

import org.apache.log4j.Appender;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

/** 
 * shameless adaptation of the org.apache.log4j.AsyncAppender
 * @author sd
 *
 */
public class CassandraAppender extends AppenderSkeleton implements Appender {

	protected String cassandraHosts = "127.0.0.1:9160";
	protected String cassandraKeyspace = "logger";
	protected String cassandraColumnFamily = "theLogs";
	protected String clusterName ="log-cluster";
	protected CassandraHostConfigurator chc = null;
	protected Cluster cluster = null;
	protected Keyspace keyspace = null;
	protected Integer bufferSize = 1;
	protected ArrayList<LoggingEvent> buffer;
	protected ArrayList<LoggingEvent> removes;
	private boolean locationInfo = false;
	StringSerializer ss = new StringSerializer();
	LongSerializer ls = new LongSerializer();
	
	public CassandraAppender() { 
		super();
		buffer = new ArrayList<LoggingEvent>(bufferSize);
		removes = new ArrayList<LoggingEvent>(bufferSize);
	}
	
	protected Keyspace getCassandraKeyspace()  {
		if (keyspace == null) {
	        chc = new CassandraHostConfigurator(cassandraHosts);
			cluster = HFactory.getOrCreateCluster(clusterName,chc);
			keyspace = HFactory.createKeyspace(cassandraKeyspace, cluster);
		}
		return keyspace;
	}	
	
	public boolean getLocationInfo() {
		return locationInfo;
	}
	  
	public void setLocationInfo(final boolean flag) {
		locationInfo = flag;
	}	
	  
	public void close() {
		flushBuffer();
	    this.closed = true;
	}

	public boolean requiresLayout() {
		return true;
	}

	protected void append(LoggingEvent event) {
	    event.getNDC();
	    event.getThreadName();
	    // Get a copy of this thread's MDC.
	    event.getMDCCopy();
	    if (locationInfo) {
	      event.getLocationInformation();
	    }
	    event.getRenderedMessage();
	    event.getThrowableStrRep();
	    buffer.add(event);

	    if (buffer.size() >= bufferSize)
	      flushBuffer();		
	}
	
	protected void save(LoggingEvent logEvent) {
		keyspace = getCassandraKeyspace();
		Mutator<String> m = HFactory.createMutator(keyspace, ss);
		m.addInsertion(logEvent.getLoggerName(), cassandraColumnFamily, HFactory.createColumn(System.nanoTime(), getLogStatement(logEvent), ls, ss));
		m.execute();
	}	
	
	public void flushBuffer() {
		//Do the actual logging
		removes.ensureCapacity(buffer.size());
		for (Iterator<LoggingEvent> i = buffer.iterator(); i.hasNext();) {
				LoggingEvent logEvent = (LoggingEvent)i.next();
				save(logEvent);
				removes.add(logEvent);
		}
		    
		// remove from the buffer any events that were reported
		buffer.removeAll(removes);
		    
		// clear the buffer of reported events
		removes.clear();
	}	
	
	protected String getLogStatement(LoggingEvent event) {
		return getLayout().format(event);
	}	

}
