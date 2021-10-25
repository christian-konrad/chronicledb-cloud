package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines;

import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.messages.ExecutableMessage;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.util.AutoCloseableLock;

import java.io.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * State machine that runs executable messages
 */
public abstract class ExecutableMessageStateMachine<StateObjectClass, ExecutableMessageImpl extends ExecutableMessage, ExecutionResultProto extends org.apache.ratis.thirdparty.com.google.protobuf.Message> extends BaseStateMachine {

    // TODO have another extending abstract class with a custom, segmented state machine storage for chronicle
    protected final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();

    // private final Supplier<? extends ExecutableMessageImpl> executableMessageSupplier;

    protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    protected AutoCloseableLock readLock() {
        return AutoCloseableLock.acquire(lock.readLock());
    }

    protected AutoCloseableLock writeLock() {
        return AutoCloseableLock.acquire(lock.writeLock());
    }

    abstract protected StateObjectClass getStateObject();

    abstract protected void clearState();

    abstract protected void restoreState(ObjectInputStream in) throws IOException, ClassNotFoundException;

    protected abstract ExecutableMessageImpl createExecutableMessageOf(ByteString byteString) throws InvalidProtocolBufferException;

    private CompletableFuture<Message> ERROR_MESSAGE = CompletableFuture.completedFuture(ExecutableMessage.ERROR_MESSAGE);

    /**
     * initialize the state machine by initializing the state machine storage and
     * calling the load method which reads the last applied command and restore it
     * in the map)
     *
     * @param server      the current server information
     * @param groupId     the cluster groupId
     * @param raftStorage the raft storage which is used to keep raft related
     *                    stuff
     * @throws IOException if any error happens during load state
     */
    @Override
    public void initialize(RaftServer server, RaftGroupId groupId,
                           RaftStorage raftStorage) throws IOException {
        super.initialize(server, groupId, raftStorage);
        this.storage.init(raftStorage);
        loadSnapshot(storage.getLatestSnapshot());
    }

    /**
     * very similar to initialize method, but doesn't initialize the storage
     * system because the state machine reinitialized from the PAUSE state and
     * storage system initialized before.
     *
     * @throws IOException if any error happens during load state
     */
    @Override
    public void reinitialize() throws IOException {
        close();
        loadSnapshot(storage.getLatestSnapshot());
    }

    void reset() {
        clearState();
        setLastAppliedTermIndex(null);
        // TODO difference to setLastAppliedTermIndex(TermIndex.valueOf(0, -1)); ?
    }

    @Override
    public void close() {
        reset();
    }

    /**
     * Load the state of the state machine from the storage.
     *
     * @param snapshot to load
     * @return the index of the snapshot or -1 if snapshot is invalid
     * @throws IOException if any error happens during read from storage
     */
    private long loadSnapshot(SingleFileSnapshotInfo snapshot) throws IOException {
        //check the snapshot nullity
        if (snapshot == null) {
            LOG.warn("The snapshot info is null.");
            return RaftLog.INVALID_LOG_INDEX;
        }

        //check the existence of the snapshot file
        final File snapshotFile = snapshot.getFile().getPath().toFile();
        if (!snapshotFile.exists()) {
            LOG.warn("The snapshot file {} does not exist for snapshot {}",
                    snapshotFile, snapshot);
            return RaftLog.INVALID_LOG_INDEX;
        }

        //load the TermIndex object for the snapshot using the file name pattern of
        // the snapshot
        final TermIndex last =
                SimpleStateMachineStorage.getTermIndexFromSnapshotFile(snapshotFile);

        //read the file, cast it to the Map and set it
        try (AutoCloseableLock writeLock = writeLock();
             ObjectInputStream in = new ObjectInputStream(
                     new BufferedInputStream(new FileInputStream(snapshotFile)))) {
            // if (reload) reset(); // TODO remove, unused
            setLastAppliedTermIndex(last);
            restoreState(in);

        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }

        return last.getIndex();
    }

    /**
     * Store the current state as a snapshot file in the stateMachineStorage
     *
     * @return the index of the snapshot
     */
    @Override
    public long takeSnapshot() {
        final TermIndex last;
        try(AutoCloseableLock readLock = readLock()) {
            last = getLastAppliedTermIndex();
        }

        final File snapshotFile = storage.getSnapshotFile(last.getTerm(), last.getIndex());
        LOG.info("Taking a snapshot to file {}", snapshotFile);

        try(AutoCloseableLock readLock = readLock();
            ObjectOutputStream out = new ObjectOutputStream(
                    new BufferedOutputStream(new FileOutputStream(snapshotFile)))) {
            out.writeObject(getStateObject());
        } catch(IOException ioe) {
            LOG.warn("Failed to write snapshot file \"" + snapshotFile
                    + "\", last applied index=" + last);
        }

        return last.getIndex();
    }

    private CompletableFuture<Message> cancelMessageExecution(ExecutableMessageImpl executableMessage) throws ExecutionException, InterruptedException {
        val cancellationResult = (ExecutionResultProto) executableMessage.cancel().get();
        return CompletableFuture.completedFuture(Message.valueOf(cancellationResult.toByteString()));
    }

    /**
     * Handle executable messages of query type
     *
     * @param request the executable message
     * @return the response message
     */
    //@SneakyThrows
    @Override
    public CompletableFuture<Message> query(Message request) {
        // TODO remove sneaky throws and add actual error handling

        // LOG.info("QUERY - Before transforming message to executable message");

        final ExecutableMessageImpl executableMessage;
        try {
            executableMessage = createExecutableMessageOf(request.getContent());
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
            // LOG.error("Extracting the executable message failed", e);
            return ERROR_MESSAGE;
        }

        // LOG.info("QUERY - After transforming message to executable message");

        // check if is valid transaction message
        if (!executableMessage.isQueryMessage() || !executableMessage.isValid()) {
            // LOG.info("QUERY - Message execution canceled");
            try {
                return cancelMessageExecution(executableMessage);
            } catch (ExecutionException | InterruptedException e) {
                // LOG.error("Cancelling of the executable message failed", e);
                return ERROR_MESSAGE;
            }
        }

        ExecutionResultProto result;

        // TODO actually could be multiple result proto types...
        // should retrieve return type from ExecutableMessageImpl via reflection
        /*
        Class<T> persistentClass = (Class<T>)
           ((ParameterizedType)getClass().getGenericSuperclass())
              .getActualTypeArguments()[0];
         */

        // LOG.info("QUERY - Before executing the message");

        try (AutoCloseableLock readLock = readLock()) {
            // actual execution of the command
            result = (ExecutionResultProto) executableMessage.apply(getStateObject()).get();
            // LOG.info("QUERY - After executing the message");
            // TODO may use response.getStatus() ?
        } catch (ExecutionException | InterruptedException e) {
            // LOG.error("Execution of the executable message failed", e);
            return ERROR_MESSAGE;
        }

        // LOG.info("QUERY - Before sending the message");

        return CompletableFuture.completedFuture(Message.valueOf(result.toByteString()));
    }

    /**
     * Apply executable message operations on the state machine
     *
     * @param trx the transaction context
     * @return the response message containing the operation result
     */
    @SneakyThrows
    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {

        final RaftProtos.LogEntryProto entry = trx.getLogEntry();
        // TODO remove sneaky throws and add actual error handling

        // TODO println for debugging

        // LOG.info("TRANSACTION - Get message from log");

        final ExecutableMessageImpl executableMessage =
                createExecutableMessageOf(entry.getStateMachineLogEntry().getLogData());

        // LOG.info("TRANSACTION - Got message from log");

        // check if is valid transaction message
        if (!executableMessage.isTransactionMessage() || !executableMessage.isValid()) {
            // LOG.info("TRANSACTION - Message execution canceled");
            // TODO also add reason?
            return cancelMessageExecution(executableMessage);
        }

        //update the last applied term and index
        final long index = entry.getIndex();

        ExecutionResultProto result;

        // LOG.info("TRANSACTION - Before executing the message");

        try(AutoCloseableLock writeLock = writeLock()) {
            // actual execution of the command
            // TODO should we actually wait?
            result = (ExecutionResultProto) executableMessage.apply(getStateObject()).get();
            // TODO may use response.getStatus() ?
            updateLastAppliedTermIndex(entry.getTerm(), index);
        }

        // LOG.info("TRANSACTION - After executing the message");

        // confirm execution (TODO or else return error)
        final CompletableFuture<Message> response =
                CompletableFuture.completedFuture(Message.valueOf(result.toByteString()));

        // LOG.info("TRANSACTION - Before wrapping in response");

        // log what happened
        final RaftProtos.RaftPeerRole role = trx.getServerRole();
        if (role == RaftProtos.RaftPeerRole.LEADER) {
            LOG.info("{}:{}-{}: {}", role, getId(), index, executableMessage);
        } else {
            LOG.debug("{}:{}-{}: {}", role, getId(), index, executableMessage);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("{}-{}: state={}", getId(), index, getStateObject());
        }

        return response;
    }
}
