package org.fastj.flow;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

public class SchedulerTask extends FTask {
	private static final AtomicLong ID = new AtomicLong();
	private int total;
	private int countPerLoop;
	private int interval;

	private TFactory taskFactory;
	private int emitCount = 0;
	private volatile boolean end = false;

	public SchedulerTask(Flow flow, int total, int cpl, int interval, TFactory tf) {
		super(flow, "ScheduleTask" + ID.incrementAndGet());
		this.emiter = new InnerEmiter();
		this.countPerLoop = cpl;
		this.total = total;
		this.interval = interval;
		this.taskFactory = tf;
		emitTasks();
		TimeUtil.schedule(() -> emitTasks(), this.interval, total / cpl + 1);
	}

	private void emitTasks() {
		int ecnt = total - emitCount;
		ecnt = ecnt > countPerLoop ? countPerLoop : ecnt;

		Flow flow = flow();

		// flow finished
		if (flow.status() > FTask.STARTED) {
			end = true;
			return;
		}

		for (int i = 0; i < ecnt; i++) {
			emitCount++;
			flow.add(taskFactory.task(flow));
		}
		Flow.schedule(() -> flow.loop());
	}

	@Override
	public void run() {
		// Schedule finished
		done();
	}

	class InnerEmiter implements Emiter {

		public int emit() {
			int e = emitCount >= total || end ? EMIT : NEXT;
			return e;
		}

		public int finish(FTask task) {
			return 0;
		}

	}

	static class TimeUtil {
		private static Timer timer = new Timer();

		public static void schedule(Runnable r, int period, int count) {
			timer.schedule(new TimerTask() {
				public void run() {
					try {
						r.run();
					} catch (Throwable e) {
					} finally {
						int c = count - 1;
						if (c > 0) {
							schedule(r, period, c);
						}
					}
				}
			}, period);
		}

	}
}
