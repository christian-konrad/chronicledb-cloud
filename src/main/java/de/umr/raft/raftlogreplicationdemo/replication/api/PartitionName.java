package de.umr.raft.raftlogreplicationdemo.replication.api;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

import javax.servlet.http.Part;
import java.util.UUID;

/**
 * Identifier to uniquely identify a Partition
 * (containing of a Name that is scoped to the name of a state machine class).
 */

@RequiredArgsConstructor(staticName = "of")
public class PartitionName {
    // It's pretty likely that what uniquely defines a Partition
    // to change over time. We should account for this with an
    // API which can naturally evolve.

    @Getter @NonNull final String stateMachineClassName;
    @Getter @NonNull final String name;
    public String getFullyQualifiedName() {
        return stateMachineClassName + ":" + name;
    }

    @Override public int hashCode() {
        return getFullyQualifiedName().hashCode();
    }

    @Override public String toString() {
        return "PartitionName['" + getFullyQualifiedName() + "']";
    }

    public static PartitionName of(String getFullyQualifiedName) throws InvalidPartitionNameException {
        String[] parts = getFullyQualifiedName.split(":");
        if (parts.length != 2) {
            throw new InvalidPartitionNameException();
        }
        return PartitionName.of(parts[0], parts[1]);
    }

//    public static PartitionName parseFrom(ByteString partitionName)
//            throws InvalidProtocolBufferException {
//        RaftProtos.PartitionNameProto partitionNameProto = RaftProtos.PartitionNameProto.parseFrom(logName);
//        return new PartitionName(partitionNameProto.getStateMachineClassName(), partitionNameProto.getName());
//    }

    private static class InvalidPartitionNameException extends Throwable {
        @Override
        public String getMessage() {
            return "Fully qualified partition name must be of the format <StateMachineClassName>:<PartitionName>";
        }
    }
}
