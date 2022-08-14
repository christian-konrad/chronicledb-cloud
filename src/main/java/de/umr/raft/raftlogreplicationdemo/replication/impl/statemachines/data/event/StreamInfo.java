package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event;

import de.umr.chronicledb.common.query.range.Range;
import de.umr.event.schema.EventSchema;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;

@Data
@AllArgsConstructor
public class StreamInfo {
	
	@NonNull
	private String name;
	
	private long eventCount;
	
	private Range<Long> timeInterval;
	
	@NonNull
	private EventSchema schema;

}
