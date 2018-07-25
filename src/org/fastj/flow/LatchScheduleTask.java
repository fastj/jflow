package org.fastj.flow;

import java.util.concurrent.atomic.AtomicLong;

public class LatchScheduleTask extends FTask {
	private static final AtomicLong ID = new AtomicLong();

	private TFactory taskFactory;
	private volatile boolean end = false;
	Latch latch;

	public LatchScheduleTask(Flow flow, int latch, TFactory tf) {
		super(flow, "LSTask" + ID.incrementAndGet());
		this.emiter = new InnerEmiter();
		this.latch = Latch.of(latch, new Runnable() {
			public void run() {
				emitTasks(1);
			}
		});
		this.taskFactory = tf;
		emitTasks(latch);
	}

	private void emitTasks(int count) {

		if (end) {
			return;
		}

		Flow flow = flow();

		// flow finished
		if (flow.status() > FTask.STARTED) {
			end = true;
			return;
		}

		while (!end && count-- > 0) {
			FTask t = taskFactory.task(flow);
			if (t != null) {
				t.emiter = latch;
				flow.add(t);
			} else {
				end = true;
			}
		}

		flow.loop();
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
