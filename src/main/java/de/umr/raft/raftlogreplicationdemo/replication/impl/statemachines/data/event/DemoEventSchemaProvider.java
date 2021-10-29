package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event;

import de.umr.event.schema.Attribute;
import de.umr.event.schema.EventSchema;
import de.umr.event.schema.SchemaProvider;

public class DemoEventSchemaProvider implements SchemaProvider {

    @Override
    public EventSchema getSchema(String s) throws IllegalArgumentException {
        // always returns the same schema for testing, dev and demo purposes
        return new EventSchema(
                new Attribute("someBool", Attribute.DataType.BOOLEAN),
                new Attribute("someInt", Attribute.DataType.INTEGER),
                new Attribute("someString", Attribute.DataType.STRING)
        );
    }
}
