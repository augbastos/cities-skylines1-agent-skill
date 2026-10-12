using System;
using System.Collections.Generic;
using System.Threading;

namespace SkylinesAgentBridge
{
    public sealed class CommandQueue
    {
        private readonly object gate = new object();
        private readonly Queue<QueuedCommand> commands = new Queue<QueuedCommand>();

        public CommandResult RunSync(Func<CommandResult> work, int timeoutMs)
        {
            return Run(work, timeoutMs, false);
        }

        // World mutations must run on the SIMULATION thread, not the main thread the queue is
        // pumped on. NetManager.CreateSegment -> PlayerNetAI.CreateSegment ->
        // ManualMilestone.Unlock -> UnlockManager.CheckMilestoneImpl dispatches to the main
        // thread, and ColossalFramework's Dispatcher throws
        // "Already in the same thread. Call directly" when it is already there.
        public CommandResult RunSyncOnSimulation(Func<CommandResult> work, int timeoutMs)
        {
            return Run(work, timeoutMs, true);
        }

        private CommandResult Run(Func<CommandResult> work, int timeoutMs, bool onSimulationThread)
        {
            QueuedCommand command = new QueuedCommand(work, onSimulationThread);

            lock (gate)
            {
                commands.Enqueue(command);
            }

            if (!command.Wait(timeoutMs))
            {
                return CommandResult.Fail("Timed out waiting for the game thread.");
            }

            return command.Result;
        }

        public void Process(int maxCount)
        {
            int processed = 0;

            while (processed < maxCount)
            {
                QueuedCommand command = null;

                lock (gate)
                {
                    if (commands.Count == 0)
                    {
                        return;
                    }

                    command = commands.Dequeue();
                }

                command.Execute();
                processed++;
            }
        }

        public void Clear()
        {
            lock (gate)
            {
                while (commands.Count > 0)
                {
                    commands.Dequeue().Cancel("Level is unloading.");
                }
            }
        }

        private sealed class QueuedCommand
        {
            private readonly Func<CommandResult> work;
            private readonly bool onSimulationThread;
            private readonly ManualResetEvent done = new ManualResetEvent(false);
            private CommandResult result;

            public QueuedCommand(Func<CommandResult> work, bool onSimulationThread)
            {
                this.work = work;
                this.onSimulationThread = onSimulationThread;
            }

            public CommandResult Result
            {
                get { return result; }
            }

            public bool Wait(int timeoutMs)
            {
                return done.WaitOne(timeoutMs, false);
            }

            public void Execute()
            {
                // Hand off and return immediately: the HTTP thread is the one waiting on `done`,
                // so blocking the main thread here would deadlock against the simulation thread.
                // The queued action still runs while the game is paused.
                if (onSimulationThread)
                {
                    try
                    {
                        ColossalFramework.Singleton<SimulationManager>.instance.AddAction(RunWork);
                    }
                    catch (Exception ex)
                    {
                        result = CommandResult.Fail("Could not queue simulation action: " +
                            ex.GetType().Name + ": " + ex.Message);
                        done.Set();
                    }
                    return;
                }

                RunWork();
            }

            private void RunWork()
            {
                try
                {
                    result = work();
                }
                catch (Exception ex)
                {
                    // The stack is the whole diagnosis when the fault is inside game code rather
                    // than in the command itself; the message alone hides the real caller.
                    result = CommandResult.Fail(ex.GetType().Name + ": " + ex.Message +
                        "\n" + ex.StackTrace);
                }
                finally
                {
                    done.Set();
                }
            }

            public void Cancel(string message)
            {
                result = CommandResult.Fail(message);
                done.Set();
            }
        }
    }
}
