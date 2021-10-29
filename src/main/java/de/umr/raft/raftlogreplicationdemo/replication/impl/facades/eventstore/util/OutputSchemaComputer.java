package de.umr.raft.raftlogreplicationdemo.replication.impl.facades.eventstore.util;

import de.umr.event.schema.EventSchema;
import de.umr.event.schema.IncompatibleTypeException;
import de.umr.jepc.v2.api.epa.Aggregator;
import de.umr.jepc.v2.api.epa.Correlator;
import de.umr.jepc.v2.api.epa.EPA;
import de.umr.jepc.v2.api.epa.Filter;
import de.umr.jepc.v2.api.epa.PatternMatcher;
import de.umr.jepc.v2.api.epa.Projection;
import de.umr.jepc.v2.api.epa.Stream;
import de.umr.jepc.v2.api.epa.UserDefinedEPA;
import de.umr.jepc.v2.api.epa.window.Window;
import de.umr.raft.raftlogreplicationdemo.replication.impl.facades.eventstore.ReplicatedChronicleEngine;

public class OutputSchemaComputer {

    public static EventSchema computeRecursive(EPA epa, ReplicatedChronicleEngine.EventStoreProvider indexes) {
        EventSchema result = null;
        if ( epa instanceof Stream)
            result = computeSchema((Stream) epa, indexes);
        else if ( epa instanceof Projection)
            result = computeSchema((Projection) epa, indexes);
        else if ( epa instanceof Filter)
            result = computeSchema((Filter) epa, indexes);
        else if ( epa instanceof Aggregator)
            result = computeSchema((Aggregator) epa, indexes);
        else if ( epa instanceof Correlator)
            result = computeSchema((Correlator) epa, indexes);
        else if ( epa instanceof PatternMatcher)
            result = computeSchema((PatternMatcher) epa, indexes);
        else if ( epa instanceof Window)
            result = computeSchema((Window) epa, indexes);
        else if ( epa instanceof UserDefinedEPA ) {
            try {
                result = computeSchema((UserDefinedEPA) epa, indexes);
            } catch (IncompatibleTypeException e) {
                throw new IllegalArgumentException(e.getMessage());
            }
        }
        else
            throw new IllegalArgumentException("Unsupported EPA: " + epa);
        return result;
    }

    private static EventSchema computeSchema(Stream epa, ReplicatedChronicleEngine.EventStoreProvider indexes) {
        return indexes.get(epa.getName()).getSchema();
    }

    private static EventSchema computeSchema(Projection epa, ReplicatedChronicleEngine.EventStoreProvider indexes) {
        return epa.computeOutputSchema(computeRecursive(epa.getInputEPAs()[0], indexes));
    }

    private static EventSchema computeSchema(Filter epa, ReplicatedChronicleEngine.EventStoreProvider indexes) {
        return epa.computeOutputSchema(computeRecursive(epa.getInputEPAs()[0], indexes));
    }

    private static EventSchema computeSchema(Aggregator epa, ReplicatedChronicleEngine.EventStoreProvider indexes) {
        return epa.computeOutputSchema(computeRecursive(epa.getInputEPAs()[0], indexes));
    }

    private static EventSchema computeSchema(Correlator epa, ReplicatedChronicleEngine.EventStoreProvider indexes) {
        EventSchema inputLeft = computeRecursive(epa.getInputEPAs()[0], indexes);
        EventSchema inputRight = computeRecursive(epa.getInputEPAs()[1], indexes);
        return epa.computeOutputSchema(inputLeft, inputRight);
    }

    private static EventSchema computeSchema(PatternMatcher epa, ReplicatedChronicleEngine.EventStoreProvider indexes) {
        return epa.computeOutputSchema(computeRecursive(epa.getInputEPAs()[0], indexes));
    }

    private static EventSchema computeSchema(Window epa, ReplicatedChronicleEngine.EventStoreProvider indexes) {
        return epa.computeOutputSchema(computeRecursive(epa.getInputEPAs()[0], indexes));
    }

    private static EventSchema computeSchema(UserDefinedEPA epa, ReplicatedChronicleEngine.EventStoreProvider indexes) throws IncompatibleTypeException {
        EPA[] inputEPAs = epa.getInputEPAs();
        EventSchema[] inputSchemas = new EventSchema[inputEPAs.length];
        for (int i = 0; i < inputEPAs.length; i++) {
            inputSchemas[i] = computeRecursive(inputEPAs[i], indexes);
        }
        return epa.computeOutputSchema(inputSchemas);
    }
}

