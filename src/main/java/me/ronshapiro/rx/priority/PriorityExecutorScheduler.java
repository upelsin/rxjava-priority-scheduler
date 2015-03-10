package me.ronshapiro.rx.priority;

import java.util.Queue;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import rx.Scheduler;
import rx.Subscription;
import rx.functions.Action0;
import rx.plugins.RxJavaPlugins;
import rx.subscriptions.CompositeSubscription;
import rx.subscriptions.MultipleAssignmentSubscription;
import rx.subscriptions.Subscriptions;

/**
 * Implementation of priority-aware scheduler. Based on {@link ExecutorScheduler}.
 
 * Created by Alexey Dmitriev <mr.alex.dmitriev@gmail.com> on 10.02.2015.
 */
public class PriorityExecutorScheduler extends Scheduler {

    final ScheduledExecutorService executor;

    private final Queue<PriorityExecutorAction> queue;

    private final int priority;

    public PriorityExecutorScheduler(ScheduledExecutorService executor,
                                     Queue<PriorityExecutorAction> queue,
                                     int priority) {

        this.executor = executor;
        this.queue = queue;
        this.priority = priority;
    }

    /**
     * @warn javadoc missing
     * @return
     */
    @Override
    public Worker createWorker() {
        return new ExecutorSchedulerWorker(executor, queue, priority);
    }

    /** Worker that schedules tasks on the executor indirectly through a trampoline mechanism. */
    static final class ExecutorSchedulerWorker extends Scheduler.Worker implements Runnable {
        final ScheduledExecutorService executor;
        // TODO: use a better performing structure for task tracking
        final CompositeSubscription tasks;

        final Queue<PriorityExecutorAction> queue;

        final AtomicInteger wip;

        final int priority;
        

        public ExecutorSchedulerWorker(ScheduledExecutorService executor,
                                       Queue<PriorityExecutorAction> queue,
                                       int priority) {
            this.executor = executor;
            this.queue = queue;
            this.wip = new AtomicInteger();
            this.tasks = new CompositeSubscription();
            this.priority = priority;
        }

        @Override
        public Subscription schedule(Action0 action) {
            if (isUnsubscribed()) {
                return Subscriptions.unsubscribed();
            }
            PriorityExecutorAction ea = new PriorityExecutorAction(action, tasks, priority);
            tasks.add(ea);
            queue.offer(ea);
            if (wip.getAndIncrement() == 0) {
                try {
                    executor.execute(this);
                } catch (RejectedExecutionException t) {
                    // cleanup if rejected
                    tasks.remove(ea);
                    wip.decrementAndGet();
                    // report the error to the plugin
                    RxJavaPlugins.getInstance().getErrorHandler().handleError(t);
                    // throw it to the caller
                    throw t;
                }
            }

            return ea;
        }

        @Override
        public void run() {
            do {
                queue.poll().run();
            } while (wip.decrementAndGet() > 0);
        }

        @Override
        public Subscription schedule(final Action0 action, long delayTime, TimeUnit unit) {
            if (delayTime <= 0) {
                return schedule(action);
            }
            if (isUnsubscribed()) {
                return Subscriptions.unsubscribed();
            }

            final MultipleAssignmentSubscription mas = new MultipleAssignmentSubscription();
            // tasks.add(mas); // Needs a removal without unsubscription

            try {
                Future<?> f = executor.schedule(new Runnable() {
                    @Override
                    public void run() {
                        if (mas.isUnsubscribed()) {
                            return;
                        }
                        mas.set(schedule(action));
                        // tasks.delete(mas); // Needs a removal without unsubscription
                    }
                }, delayTime, unit);
                mas.set(Subscriptions.from(f));
            } catch (RejectedExecutionException t) {
                // report the rejection to plugins
                RxJavaPlugins.getInstance().getErrorHandler().handleError(t);
                throw t;
            }

            return mas;
        }

        @Override
        public boolean isUnsubscribed() {
            return tasks.isUnsubscribed();
        }

        @Override
        public void unsubscribe() {
            tasks.unsubscribe();
        }

    }

    /** Runs the actual action and maintains an unsubscription state. */
    static final class PriorityExecutorAction
            implements Runnable, Subscription, Comparable<PriorityExecutorAction> {

        final Action0 actual;
        final CompositeSubscription parent;
        volatile int unsubscribed;
        static final AtomicIntegerFieldUpdater<PriorityExecutorAction> UNSUBSCRIBED_UPDATER
                = AtomicIntegerFieldUpdater.newUpdater(PriorityExecutorAction.class, "unsubscribed");

        private final int priority;

        public PriorityExecutorAction(Action0 actual, CompositeSubscription parent, int priority) {
            this.actual = actual;
            this.parent = parent;
            this.priority = priority;
        }

        @Override
        public void run() {
            if (isUnsubscribed()) {
                return;
            }
            try {
                actual.call();
            } catch (Throwable t) {
                RxJavaPlugins.getInstance().getErrorHandler().handleError(t);
                Thread thread = Thread.currentThread();
                thread.getUncaughtExceptionHandler().uncaughtException(thread, t);
            } finally {
                unsubscribe();
            }
        }

        @Override
        public boolean isUnsubscribed() {
            return unsubscribed != 0;
        }

        @Override
        public void unsubscribe() {
            if (UNSUBSCRIBED_UPDATER.compareAndSet(this, 0, 1)) {
                parent.remove(this);
            }
        }

        @Override
        public int compareTo(PriorityExecutorAction other) {
            return other.priority - this.priority;
        }
    }
}
