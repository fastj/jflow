package org.fastj.flow;

public class JoinPointer extends FTask {

	private Latch latch = new Latch(1);

	public JoinPointer(Flow flow, String name) {
		super(flow, name);
		latch.up();
	}

	public void run() {
		latch.down();
		done();
	}

	public boolean isDone() {
		return latch.value() <= 0;
	}
}
