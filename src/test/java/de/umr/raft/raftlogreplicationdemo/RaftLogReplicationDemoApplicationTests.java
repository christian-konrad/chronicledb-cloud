package de.umr.raft.raftlogreplicationdemo;

import de.umr.event.Event;
import de.umr.event.impl.SimpleEvent;
import de.umr.event.schema.Attribute;
import de.umr.raft.raftlogreplicationdemo.replication.impl.facades.eventstore.ReplicatedChronicleEngine;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@SpringBootTest
class RaftLogReplicationDemoApplicationTests {

	@Test
	void contextLoads() {
	}

}
