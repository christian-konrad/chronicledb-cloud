package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.query;

import de.umr.chronicledb.common.query.range.Range;
import de.umr.jepc.v2.api.epa.EPA;
import lombok.Data;

@Data
public class QueryDescription {

    private final String		queryId;
    private final EPA			epa;
    private final Range<Long>   temporalRange;

    public QueryDescription(String queryId, EPA epa) {
        this(queryId, epa, new Range<>(Long.MIN_VALUE,Long.MAX_VALUE,true,true));
    }

    public QueryDescription(String queryId, EPA epa, Range<Long> temporalRange) {
        this.queryId = queryId;
        this.epa = epa;
        this.temporalRange = temporalRange;
    }
}