package raft

import kotlin.math.max
import kotlin.math.min

/**
 * Raft algorithm implementation.
 * All functions are called from the single main thread.
 *
 * @author Timofey Belousov
 */
class ProcessImpl(private val env: Environment) : Process {
    private val storage = env.storage
    private val machine = env.machine

    private var role = Role.FOLLOWER
    private var confirmations = 0

    private val quorum = env.nProcesses / 2 + 1

    private var commitIndex = 0

    private lateinit var nextIndex: MutableList<Int>
    private lateinit var matchIndex: MutableList<Int>

    private var leaderId: Int? = null
    private val commandQueue = ArrayDeque<Command>()
    private val leaderCommandQueue = mutableListOf<Command>()

    enum class Role {
        FOLLOWER,
        CANDIDATE,
        LEADER,
    }

    init {
        if (quorum <= 1) {
            throw IllegalStateException("Empty quorum is unsupported")
        }
        env.startTimeout(Timeout.ELECTION_TIMEOUT)
    }

    override fun onTimeout() {
        when (role) {
            Role.FOLLOWER -> startNewElection()
            Role.CANDIDATE -> startNewElection()
            Role.LEADER -> {
                val state = storage.readPersistentState()

                for (i in 1..env.nProcesses) {
                    if (i != env.processId) {
                        if (nextIndex[i] > 1) {
                            val prevEntry = storage.readLog(nextIndex[i] - 1)!!
                            env.send(i, Message.AppendEntryRpc(state.currentTerm, prevEntry.id, commitIndex, null))
                        } else {
                            env.send(i, Message.AppendEntryRpc(state.currentTerm, START_LOG_ID, commitIndex, null))
                        }
                    }
                }

                env.startTimeout(Timeout.LEADER_HEARTBEAT_PERIOD)
            }
        }
    }

    private fun startNewElection() {
        val state = storage.readPersistentState()
        storage.writePersistentState(PersistentState(state.currentTerm + 1, env.processId))

        confirmations = 1
        leaderId = null
        role = Role.CANDIDATE
        env.startTimeout(Timeout.ELECTION_TIMEOUT)

        val lastLogId = storage.readLastLogId()

        for (i in 1..env.nProcesses) {
            if (i != env.processId) {
                env.send(i, Message.RequestVoteRpc(state.currentTerm + 1, lastLogId))
            }
        }
    }

    override fun onMessage(srcId: Int, message: Message) {
        val state = storage.readPersistentState()
        if (message.term > state.currentTerm) {
            storage.writePersistentState(PersistentState(message.term))

            confirmations = 0
            leaderId = null
            role = Role.FOLLOWER
            env.startTimeout(Timeout.ELECTION_TIMEOUT)
        }
        val newState = storage.readPersistentState()

        when (message) {
            is Message.AppendEntryResult -> {
                if (role != Role.LEADER) {
                    return
                }

                if (message.term < newState.currentTerm) {
                    return
                }

                if (message.lastIndex == null) {
                    nextIndex[srcId] = max(1, nextIndex[srcId] - 1)
                    replicateTo(srcId)
                } else {
                    val lastLogId = storage.readLastLogId()
                    nextIndex[srcId] = message.lastIndex + 1
                    matchIndex[srcId] = message.lastIndex
                    if (nextIndex[srcId] <= lastLogId.index) {
                        replicateTo(srcId)
                    }

                    updateLeaderCommitIfNeeded()
                }
            }

            is Message.AppendEntryRpc -> {
                if (message.term < newState.currentTerm) {
                    env.send(srcId, Message.AppendEntryResult(newState.currentTerm, null))
                    return
                }

                if (role == Role.LEADER) {
                    throw IllegalStateException("Unreachable")
                }

                env.startTimeout(Timeout.ELECTION_TIMEOUT)

                leaderId = srcId
                while (commandQueue.isNotEmpty()) {
                    env.send(srcId, Message.ClientCommandRpc(newState.currentTerm, commandQueue.removeFirst()))
                }

                if (message.prevLogId.index == 0) {
                    if (message.entry == null) {
                        env.send(srcId, Message.AppendEntryResult(newState.currentTerm, 0))
                        return
                    }
                    storage.appendLogEntry(message.entry)
                    updateCommitIndex(message, message.entry.id.index)
                    env.send(srcId, Message.AppendEntryResult(newState.currentTerm, message.entry.id.index))
                    return
                }

                val curLog = storage.readLog(message.prevLogId.index)
                if (curLog == null || curLog.id != message.prevLogId
                ) {
                    env.send(srcId, Message.AppendEntryResult(newState.currentTerm, null))
                    return
                }

                if (message.entry == null) {
                    updateCommitIndex(message, curLog.id.index)
                    env.send(srcId, Message.AppendEntryResult(newState.currentTerm, curLog.id.index))
                    return
                }

                storage.appendLogEntry(message.entry)
                updateCommitIndex(message, message.entry.id.index)
                env.send(srcId, Message.AppendEntryResult(newState.currentTerm, message.entry.id.index))
            }

            is Message.ClientCommandResult -> {
                env.onClientCommandResult(message.result)

                leaderId = srcId
                while (commandQueue.isNotEmpty()) {
                    env.send(srcId, Message.ClientCommandRpc(newState.currentTerm, commandQueue.removeFirst()))
                }
            }

            is Message.ClientCommandRpc -> {
                when (role) {
                    Role.FOLLOWER, Role.CANDIDATE -> {
                        val curLeaderId = leaderId
                        if (curLeaderId != null) {
                            env.send(curLeaderId, Message.ClientCommandRpc(newState.currentTerm, message.command))
                        } else {
                            commandQueue.addLast(message.command)
                        }
                    }

                    Role.LEADER -> {
                        val lastLogId = storage.readLastLogId()
                        val entry = LogEntry(LogId(lastLogId.index + 1, newState.currentTerm), message.command)

                        storage.appendLogEntry(entry)
                        nextIndex[env.processId] = lastLogId.index + 2
                        matchIndex[env.processId] = lastLogId.index + 1

                        leaderCommandQueue.add(message.command)

                        updateLeaderCommitIfNeeded()
                        for (i in 1..env.nProcesses) {
                            if (i != env.processId) {
                                if (nextIndex[i] == lastLogId.index + 1) {
                                    replicateTo(i)
                                }
                            }
                        }
                    }
                }
            }

            is Message.RequestVoteResult -> {
                when (role) {
                    Role.CANDIDATE -> {
                        if (message.voteGranted) {
                            confirmations++
                        }
                        if (confirmations >= quorum) {
                            confirmations = 0
                            role = Role.LEADER
                            leaderId = env.processId

                            while (commandQueue.isNotEmpty()) {
                                env.send(
                                    env.processId,
                                    Message.ClientCommandRpc(newState.currentTerm, commandQueue.removeFirst())
                                )
                            }

                            val lastId = storage.readLastLogId()
                            nextIndex = MutableList(env.nProcesses + 1) { lastId.index + 1 }
                            matchIndex = MutableList(env.nProcesses + 1) { 0 }

                            onTimeout()
                        }
                    }

                    Role.FOLLOWER, Role.LEADER -> {}
                }
            }

            is Message.RequestVoteRpc -> {
                val lastLog = storage.readLastLogId()
                if (message.term < newState.currentTerm
                    || (newState.votedFor != srcId && newState.votedFor != null)
                    || lastLog > message.lastLogId
                ) {
                    env.send(srcId, Message.RequestVoteResult(newState.currentTerm, false))
                    return
                }
                storage.writePersistentState(PersistentState(newState.currentTerm, srcId))
                env.send(srcId, Message.RequestVoteResult(newState.currentTerm, true))
            }
        }
    }

    private fun updateLeaderCommitIfNeeded() {
        val lastLogId = storage.readLastLogId()
        val state = storage.readPersistentState()
        var newCommitIndex = commitIndex

        for (i in commitIndex + 1..lastLogId.index) {
            val matchedCount = matchIndex.count { curMatched -> curMatched >= i }
            if (matchedCount < quorum) {
                break
            }
            if (storage.readLog(i)!!.id.term == state.currentTerm) {
                newCommitIndex = i
            }
        }

        for (i in commitIndex + 1..newCommitIndex) {
            val curCommand = storage.readLog(i)!!.command
            val result = machine.apply(curCommand)
            commitIndex++

            val commandIndex = leaderCommandQueue.indexOf(curCommand)
            if (commandIndex >= 0) {
                leaderCommandQueue.removeAt(commandIndex)
                if (curCommand.processId == env.processId) {
                    env.onClientCommandResult(result)
                } else {
                    env.send(curCommand.processId, Message.ClientCommandResult(state.currentTerm, result))
                }
            }
        }
        commitIndex = newCommitIndex
    }

    private fun replicateTo(srcId: Int) {
        if (nextIndex[srcId] < 1) {
            throw IllegalStateException("Unreachable")
        }
        val targetEntry = storage.readLog(nextIndex[srcId])!!
        val state = storage.readPersistentState()
        if (nextIndex[srcId] > 1) {
            val prevEntry = storage.readLog(nextIndex[srcId] - 1)!!
            env.send(srcId, Message.AppendEntryRpc(state.currentTerm, prevEntry.id, commitIndex, targetEntry))
        } else {
            env.send(srcId, Message.AppendEntryRpc(state.currentTerm, START_LOG_ID, commitIndex, targetEntry))
        }
    }

    private fun updateCommitIndex(message: Message.AppendEntryRpc, newEntryId: Int) {
        if (message.leaderCommit > commitIndex) {
            val newCommitIndex = min(message.leaderCommit, newEntryId)

            for (i in commitIndex + 1..newCommitIndex) {
                machine.apply(storage.readLog(i)!!.command)
            }

            commitIndex = newCommitIndex
        }
    }

    override fun onClientCommand(command: Command) {
        val state = storage.readPersistentState()
        onMessage(env.processId, Message.ClientCommandRpc(state.currentTerm, command))
    }
}
