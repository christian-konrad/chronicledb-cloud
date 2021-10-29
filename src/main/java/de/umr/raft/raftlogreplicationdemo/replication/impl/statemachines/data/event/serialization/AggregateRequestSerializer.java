package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.serialization;

import de.umr.chronicledb.common.query.range.Range;
import de.umr.chronicledb.event.store.tabPlus.aggregation.impl.EventAggregate;
import de.umr.chronicledb.event.store.tabPlus.aggregation.impl.attribute.AttributeAggregate;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.AggregateRequestProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.AggregateTypeProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.EventAggregateProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.RangeProto;
import lombok.val;

import java.util.List;
import java.util.stream.Collectors;

public class AggregateRequestSerializer {
    public static RangeProto toProto(Range<Long> range) {
        return RangeProto.newBuilder()
                .setLower(range.getLower())
                .setUpper(range.getUpper())
                .setLowerInclusive(range.isLowerInclusive())
                .setUpperInclusive(range.isUpperInclusive())
                .build();
    }

    public static AggregateTypeProto toAggregateTypeProto(String aggregateType) {
        try {
            return AggregateTypeProto.valueOf(aggregateType);
        } catch (IllegalArgumentException e) {
            return AggregateTypeProto.UNRECOGNIZED;
        }
    }

    public static EventAggregateProto toProto(EventAggregate eventAggregate) {
        val aggregateTypeProto = toAggregateTypeProto(eventAggregate.getAggregateId());

        val builder = EventAggregateProto.newBuilder()
                .setAggregateId(aggregateTypeProto);

        if (eventAggregate instanceof AttributeAggregate) {
            builder.setAttribute(((AttributeAggregate) eventAggregate).getAttribute());
        }

        return builder.build();
    }

    public static List<EventAggregateProto> toProto(List<? extends EventAggregate> eventAggregates) {
        return eventAggregates.stream().map(AggregateRequestSerializer::toProto).collect(Collectors.toList());
    }

    public static AggregateRequestProto toProto(Range<Long> range, List<? extends EventAggregate> list) {
        val rangeProto = toProto(range);
        val eventAggregateProtos = toProto(list);
        return AggregateRequestProto.newBuilder()
                .setRange(rangeProto)
                .addAllEventAggregates(eventAggregateProtos)
                .build();
    }
}
