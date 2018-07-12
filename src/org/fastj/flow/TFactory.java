package org.fastj.flow;

public abstract class TFactory {
	private Emiter emiter = null;

	public TFactory(Emiter emiter) {
		this.emiter = emiter;
	}

	FTask task(Flow flow) {

		Runnable r = task();

		return new FTask(flow, emiter) {
			public void run() {
				try {
					if (r != null) {
						r.run();
					}
					done();
				} catch (Throwable e) {
					error(e);
				}
			}
		};
	}

	public abstract Runnable task();
}