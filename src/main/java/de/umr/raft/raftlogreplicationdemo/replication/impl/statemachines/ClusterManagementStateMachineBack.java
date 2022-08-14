package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines;

import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.util.AutoCloseableLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Deprecated
public class ClusterManagementStateMachineBack extends BaseStateMachine {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterManagementStateMachineBack.class);

    // TODO private final ClusterState <-- all that is to be known about the cluster

    // TODO may be unused
    private final SimpleStateMachineStorage storage =
            new SimpleStateMachineStorage();

    // List of the currently known peers
    private final Set<RaftPeer> peers = new HashSet<>();

    // TODO maybe something leaner than the whole provider
    //  (as group members can change over time and be different from the initial ones
    //  and we only need group name and state machine type)
    // TODO maybe PartitionName?


    // private Map<PartitionName, RaftGroup> map = new ConcurrentHashMap<>();

    // TODO maps/lists of nodes, raftGroups, node health etc

    // TODO total list of peers
    // TODO current list of groups with according peers (may stateMachineProviders?)


    // TODO afterwards, remove from metadatamap

    // TODO implement health checks based on heartbeats

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
        final TermIndex last;
        try(AutoCloseableLock readLock = readLock()) {
            // TODO clone snapshot payload
            last = getLastAppliedTermIndex();
        }

        //create a file with a proper name to store the snapshot
        final File snapshotFile =
                storage.getSnapshotFile(last.getTerm(), last.getIndex());
        LOG.info("Taking a snapshot to file {}", snapshotFile);

        //serialize the counter object and write it into the snapshot file
        try (ObjectOutputStream out = new ObjectOutputStream(
                new BufferedOutputStream(new FileOutputStream(snapshotFile)))) {
            // out.writeObject(copy); TODO
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

        try (AutoCloseableLock writeLock = writeLock();
            ObjectInputStream in = new ObjectInputStream(
                    new BufferedInputStream(new FileInputStream(snapshotFile)))) {
            setLastAppliedTermIndex(last);
            // TODO rebuild state
            // metadata.putAll(JavaUtils.cast(in.readObject()));
        }
        /*
        catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
        */

        return last.getIndex();
    }

    /**
     * Handles queries
     *
     * @param request
     * @return
     */
    @Override
    public CompletableFuture<Message> query(Message request) {
        String msg = request.getContent().toString(Charset.defaultCharset());

//        if (!msg.equals("GET")) {
//            return CompletableFuture.completedFuture(
//                    Message.valueOf("Invalid Command"));
//        }
//        byte[] metadataBytes = SerializationUtils.serialize(metadata);
//
//        return CompletableFuture.completedFuture(
//                Message.valueOf(ByteString.copyFrom(metadataBytes)));

        // TODO

        return CompletableFuture.completedFuture(Message.valueOf("NOOP"));
    }

    /**
     * Apply operations
     *
     * @param trx the transaction context
     * @return the message containing the operation
     */
    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        final RaftProtos.LogEntryProto entry = trx.getLogEntry();
//        final MetadataOperationMessage metaDataOperationMessage =
//                new MetadataOperationMessage(entry.getStateMachineLogEntry().getLogData());

        // TODO may catch exception and return message to the user if message is invalid

        //update the last applied term and index
        final long index = entry.getIndex();

        try(AutoCloseableLock writeLock = writeLock()) {
            // actual execution of the command
//            metaDataOperationMessage.apply(metadata);
            updateLastAppliedTermIndex(entry.getTerm(), index);
        }

        // confirm execution (TODO or else return error)
        final CompletableFuture<Message> response =
                CompletableFuture.completedFuture(Message.valueOf("OK"));

        // log what happened
        final RaftProtos.RaftPeerRole role = trx.getServerRole();
        if (role == RaftProtos.RaftPeerRole.LEADER) {
//            LOG.info("{}:{}-{}: {}", role, getId(), index, metaDataOperationMessage);
        } else {
//            LOG.debug("{}:{}-{}: {}", role, getId(), index, metaDataOperationMessage);
        }
        if (LOG.isTraceEnabled()) {
//            LOG.trace("{}-{}: metadata={}", getId(), index, metadata);
        }

        return response;
    }
}
