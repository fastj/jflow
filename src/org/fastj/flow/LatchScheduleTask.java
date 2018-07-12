package org.fastj.flow;

import java.util.concurrent.atomic.AtomicLong;

public class LatchScheduleTask extends FTask {
	private static final AtomicLong ID = new AtomicLong();

	private TFactory taskFactory;
	private int emitCount = 0;
	private volatile boolean end = false;
	Latch latch;

	public LatchScheduleTask(Flow flow, int latch, TFactory tf) {
		super(flow, "ScheduleTask" + ID.incrementAndGet());
		this.emiter = new InnerEmiter();
		this.latch = Latch.of(latch, new Runnable() {
			public void run() {
				emitTasks();
			}
		});
		this.taskFactory = tf;
		emitTasks();
	}

	private void emitTasks() {
		Flow flow = flow();

		// flow finished
		if (flow.status() > FTask.STARTED) {
			end = true;
			return;
		}

		FTask t = taskFactory.task(flow);
		if (t != null) {
			emitCount++;
			t.emiter = latch;
			flow.add(t);
		} else {
			end = true;
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
			int e = end ? EMIT : NEXT;
			return e;
		}

		public int finish(FTask task) {
			return 0;
		}

	}
}
