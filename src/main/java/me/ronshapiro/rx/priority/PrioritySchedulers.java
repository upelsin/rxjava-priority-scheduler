package me.ronshapiro.rx.priority;

import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;

import rx.Scheduler;

import static me.ronshapiro.rx.priority.PriorityExecutorScheduler.PriorityExecutorAction;

/**
 * Created by Alexey Dmitriev <mr.alex.dmitriev@gmail.com> on 04.02.2015.
 */
public class PrioritySchedulers {

    private static Queue<PriorityExecutorAction> QUEUE = new PriorityBlockingQueue<PriorityExecutorAction>();

    private static ScheduledExecutorService EXECUTOR = Executors.newSingleThreadScheduledExecutor();

    public static Scheduler priority(int priority) {
        return new PriorityExecutorScheduler(EXECUTOR, QUEUE, priority);
    }
}
