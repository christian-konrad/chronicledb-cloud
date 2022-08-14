package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event;

import de.umr.chronicledb.event.store.tabPlus.aggregation.EventAggregationValues;
import de.umr.chronicledb.event.store.tabPlus.aggregation.impl.EventAggregate;

import java.util.Map;
import java.util.Optional;

public final class FinalizedEventAggregationValues extends EventAggregationValues {

    private final Map<String, Optional<?>> values;

    public FinalizedEventAggregationValues(Map<String, Optional<?>> values) {
        super(null, null, null);
        this.values = values;
    }

    /* In this finalized implementation, the values are already beeing computed */
    public Optional<?> getAggregationValue(EventAggregate aggregate) throws IllegalArgumentException {
        // TODO aggregate.getInstanceId is for some reason protected; would be better to call it directly instead of toString
        return values.get(aggregate.toString());
    }

    @Override
    public String toString() {
        return values == null ? "{}" : values.toString();
    }
}
