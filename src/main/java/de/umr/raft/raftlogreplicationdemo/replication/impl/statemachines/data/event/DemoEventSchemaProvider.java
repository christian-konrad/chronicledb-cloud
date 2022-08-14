package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event;

import de.umr.event.schema.Attribute;
import de.umr.event.schema.EventSchema;
import de.umr.event.schema.SchemaProvider;

public class DemoEventSchemaProvider implements SchemaProvider {

    @Override
    public EventSchema getSchema(String s) throws IllegalArgumentException {
        // always returns the same schema for testing, dev and demo purposes
        if ("CEBS2022".equals(s)) {
            return new EventSchema(
                    // Unique identifier for this symbol with respective exchange: Paris (FR), Amsterdam (NL) or Frankfurt (ETR)
                    new Attribute("symbol", Attribute.DataType.STRING),
                    // Enum; Security type: [E]quity (e.g., Royal Dutch Shell, Siemens Healthineers) or [I]ndex (e.g., DAX)
                    new Attribute("securityType", Attribute.DataType.INTEGER),
                    // price with 6 digits ##.####
                    new Attribute("lastTradePrice", Attribute.DataType.FLOAT)
                    // timestamp of last trade, bid or ask (t1)
                    // new Attribute("lastTrade", Attribute.DataType.STRING)
            );
        }
        return new EventSchema(
                new Attribute("someBool", Attribute.DataType.BOOLEAN),
                new Attribute("someInt", Attribute.DataType.INTEGER),
                new Attribute("someString", Attribute.DataType.STRING)
        );
    }
}
