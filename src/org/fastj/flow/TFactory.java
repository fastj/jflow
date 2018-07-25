package org.fastj.flow;

public abstract class TFactory {
	private Emiter emiter = null;
	private boolean compute = false;

	public TFactory(Emiter emiter) {
		this.emiter = emiter;
	}

	public TFactory(Emiter emiter, boolean compute) {
		this.emiter = emiter;
		this.compute = compute;
	}

	FTask task(Flow flow) {
		Runnable r = task();

		if (r == null) {
			return null;
		}

		if (compute) {
			return new Flow.ComputeTask(flow, r);
		}

		return new FTask(flow, emiter) {
			public void run() {
				try {
					r.run();
					done();
				} catch (Throwable e) {
					error(e);
				}
			}
		};
	}

	public abstract Runnable task();
}
