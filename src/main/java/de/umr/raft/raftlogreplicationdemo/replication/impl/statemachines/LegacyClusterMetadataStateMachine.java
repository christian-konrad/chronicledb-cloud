package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines;

import de.umr.raft.raftlogreplicationdemo.replication.api.proto.MetadataOperationResultProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.messages.ExecutableMessage;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.messages.metadata.MetadataOperationMessage;
import lombok.SneakyThrows;
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
import org.apache.ratis.util.AutoCloseableLock;
import org.apache.ratis.util.JavaUtils;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Deprecated
public class LegacyClusterMetadataStateMachine extends BaseStateMachine {
    private final SimpleStateMachineStorage storage =
            new SimpleStateMachineStorage();

    // a map of key value pairs per scope (e.g. node id)

    // TODO need AtomicReference? Or even some KV in-mem database? Or some custom object?
    // private AtomicReference<Map<String, Map<String, String>>> metadata = new AtomicReference<>(new HashMap<>());
    private final Map<String, Map<String, String>> metadata = new ConcurrentHashMap<>();

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    private AutoCloseableLock readLock() {
        return AutoCloseableLock.acquire(lock.readLock());
    }

    private AutoCloseableLock writeLock() {
        return AutoCloseableLock.acquire(lock.writeLock());
    }

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
        metadata.clear();
        setLastAppliedTermIndex(null);
    }

    @Override
    public void close() {
        reset();
    }

    /**
     * Store the current state as an snapshot file in the stateMachineStorage.
     *
     * @return the index of the snapshot
     */
    @Override
    public long takeSnapshot() {
        final Map<String, Map<String, String>> copy;
        final TermIndex last;
        try(AutoCloseableLock readLock = readLock()) {
            copy = new HashMap<>(metadata);
            last = getLastAppliedTermIndex();
        }

        //create a file with a proper name to store the snapshot
        final File snapshotFile =
                storage.getSnapshotFile(last.getTerm(), last.getIndex());
        LOG.info("Taking a snapshot to file {}", snapshotFile);

        //serialize the counter object and write it into the snapshot file
        try (ObjectOutputStream out = new ObjectOutputStream(
                new BufferedOutputStream(new FileOutputStream(snapshotFile)))) {
            out.writeObject(copy);
        } catch (IOException ioe) {
            LOG.warn("Failed to write snapshot file \"" + snapshotFile
                    + "\", last applied index=" + last);
        }

        //return the index of the stored snapshot (which is the last applied one)
        return last.getIndex();
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
            metadata.putAll(JavaUtils.cast(in.readObject()));
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }

        return last.getIndex();
    }

    /**
     * Handle queries on the map. Currently, the whole map is returned
     *
     * @param request the GET message
     * @return the Message containing the current counter value
     */
    @SneakyThrows
    @Override
    public CompletableFuture<Message> query(Message request) {
        // TODO remove sneaky throws and add actual error handling

        // TODO cast or constructor?
//        final MetadataOperationMessage metaDataOperationMessage =
//                MetadataOperationMessage.of(request.getContent());

        final MetadataOperationMessage metaDataOperationMessage = (MetadataOperationMessage) request;

        // check if is valid transaction message
        if (!metaDataOperationMessage.isQueryMessage() || !metaDataOperationMessage.isValid()) {
            //return CompletableFuture.completedFuture(ExecutableMessage.INVALID_MESSAGE);
        }

        // String msg = request.getContent().toString(Charset.defaultCharset());

        MetadataOperationResultProto result;

        try(AutoCloseableLock readLock = readLock()) {
            // actual execution of the command
            result = metaDataOperationMessage.apply(metadata).get();
            // TODO may use response.getStatus() ?
        }

        //byte[] metadataBytes = SerializationUtils.serialize(metadata);

        // return CompletableFuture.completedFuture(
        //        Message.valueOf(ByteString.copyFrom(metadataBytes)));

        return CompletableFuture.completedFuture(Message.valueOf(result.toByteString()));
    }

    /**
     * Apply put and delete operations on the map
     *
     * @param trx the transaction context
     * @return the message containing the operation
     */
    @SneakyThrows
    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        // TODO generalize this as we always use the same pattern; simply add type params to a new simplified statemachine class

        final RaftProtos.LogEntryProto entry = trx.getLogEntry();
        // TODO remove sneaky throws and add actual error handling
        final MetadataOperationMessage metaDataOperationMessage =
                MetadataOperationMessage.of(entry.getStateMachineLogEntry().getLogData());

        // check if is valid transaction message
        if (!metaDataOperationMessage.isTransactionMessage() || !metaDataOperationMessage.isValid()) {
            //return CompletableFuture.completedFuture(ExecutableMessage.INVALID_MESSAGE);
        }

        //update the last applied term and index
        final long index = entry.getIndex();

        MetadataOperationResultProto result;

        try(AutoCloseableLock writeLock = writeLock()) {
            // actual execution of the command
            // TODO should we actually wait?
            result = metaDataOperationMessage.apply(metadata).get();
            // TODO may use response.getStatus() ?
            //metaDataOperationMessage.apply(metadata).join();
            updateLastAppliedTermIndex(entry.getTerm(), index);
        }

        // confirm execution (TODO or else return error)
        final CompletableFuture<Message> response =
                CompletableFuture.completedFuture(Message.valueOf(result.toByteString()));

        // log what happened
        final RaftProtos.RaftPeerRole role = trx.getServerRole();
        if (role == RaftProtos.RaftPeerRole.LEADER) {
            LOG.info("{}:{}-{}: {}", role, getId(), index, metaDataOperationMessage);
        } else {
            LOG.debug("{}:{}-{}: {}", role, getId(), index, metaDataOperationMessage);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("{}-{}: metadata={}", getId(), index, metadata);
        }

        return response;
    }
}
