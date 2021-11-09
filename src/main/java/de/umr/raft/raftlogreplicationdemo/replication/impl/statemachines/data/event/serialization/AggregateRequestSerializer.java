package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.serialization;

import de.umr.chronicledb.common.query.range.Range;
import de.umr.chronicledb.event.store.tabPlus.aggregation.impl.EventAggregate;
import de.umr.chronicledb.event.store.tabPlus.aggregation.impl.attribute.*;
import de.umr.chronicledb.event.store.tabPlus.aggregation.impl.global.EventCount;
import de.umr.chronicledb.event.store.tabPlus.aggregation.impl.global.GlobalAggregate;
import de.umr.jepc.v2.api.epa.aggregates.*;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.*;
import lombok.val;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class AggregateRequestSerializer {
    public static RangeProto toProto(Range<Long> range) {
        return RangeProto.newBuilder()
                .setLower(range.getLower())
                .setUpper(range.getUpper())
                .setIsLowerInclusive(range.isLowerInclusive())
                .setIsUpperInclusive(range.isUpperInclusive())
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
            builder.setAttribute(((AttributeAggregate) eventAggregate).getAttribute())
                    .setAggregateScope(AggregateScopeProto.ATTRIBUTE);
        }

        if (eventAggregate instanceof GlobalAggregate) {
            builder.setAggregateScope(AggregateScopeProto.GLOBAL);
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

    public static Range<Long> fromProto(RangeProto rangeProto) {
        val lower = rangeProto.getLower();
        val upper = rangeProto.getUpper();
        val isLowerInclusive = rangeProto.getIsLowerInclusive();
        val isUpperInclusive = rangeProto.getIsUpperInclusive();

        return new Range<>(lower, upper, isLowerInclusive, isUpperInclusive);
    }

    public static AttributeAggregate attributeAggregateFromProto(EventAggregateProto aggregateProto) {
        val attr = aggregateProto.getAttribute();

        switch (aggregateProto.getAggregateId()) {
            case COUNT:
                return new AttributeCount(attr);
            case SUM:
                return new AttributeSum(attr);
            case MIN:
                return new AttributeMin(attr);
            case MAX:
                return new AttributeMax(attr);
            case BBOX:
                return new AttributeBoundingBox(attr);
            case UNRECOGNIZED:
            case UNKNOWN_AGGREGATE:
            default:
                throw new UnsupportedOperationException("Unknown attribute aggregate type");
        }
    }

    public static GlobalAggregate globalAggregateFromProto(EventAggregateProto aggregateProto) {
        switch (aggregateProto.getAggregateId()) {
            case COUNT:
                return new EventCount();
            case UNRECOGNIZED:
            case UNKNOWN_AGGREGATE:
            default:
                throw new UnsupportedOperationException("Unknown attribute aggregate type");
        }
    }

    public static EventAggregate fromProto(EventAggregateProto aggregateProto) {
        switch (aggregateProto.getAggregateScope()) {
            case ATTRIBUTE:
                return attributeAggregateFromProto(aggregateProto);
            case GLOBAL:
                return globalAggregateFromProto(aggregateProto);
            case UNRECOGNIZED:
            case UNKNOWN_SCOPE:
            default:
                throw new UnsupportedOperationException("Unknown event aggregate scope");
        }
    }

    public static List<? extends EventAggregate> fromProto(List<EventAggregateProto> aggregateProtos) {
        return aggregateProtos.stream().map(AggregateRequestSerializer::fromProto).collect(Collectors.toList());
    }

    public static Optional<?> fromProto(AggregationValueProto value) {
        if (value.getIsNull()) {
            return Optional.empty();
        }
        switch (value.getValueType()) {
            case FLOAT:
                return Optional.of(value.getFloatValue());
            case LONG:
                return Optional.of(value.getLongValue());
            case UNKNOWN_VALUE_TYPE:
            case UNRECOGNIZED:
            default:
                throw new UnsupportedOperationException("Unsupported aggregation value type");
        }
    }
}
