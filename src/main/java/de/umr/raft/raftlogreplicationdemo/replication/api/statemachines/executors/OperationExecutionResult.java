package de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.ratis.thirdparty.com.google.protobuf.Message;

@AllArgsConstructor(staticName = "of")
@Deprecated
public class OperationExecutionResult<ExecutionResultTypeProto extends Message> {

    @Getter private ExecutionResultTypeProto value;
    @Getter @NonNull private Boolean isSuccessful;
}
