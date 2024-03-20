package raft

import raft.Message.*

/**
 * Raft algorithm implementation.
 * All functions are called from the single main thread.
 *
 * @author <First-Name> <Last-Name> // todo: replace with your name
 */
class ProcessImpl(private val env: Environment) : Process {
    private val storage = env.storage
    private val machine = env.machine

    override fun onTimeout() {
        /* todo: write implementation here */
    }

    override fun onMessage(srcId: Int, message: Message) {
        /* todo: write implementation here */
    }

    override fun onClientCommand(command: Command) {
        /* todo: write implementation here */
    }
}
