package org.fastj.flow;

public class Latch implements Emiter {

	private final Integer max;
	private volatile int current = 0;
	private Runnable fr;

	public static Latch of(int max) {
		return new Latch(max);
	}

	public static Latch of(int max, Runnable r) {
		Latch l = new Latch(max);
		l.fr = r;
		return l;
	}

	Latch(int v) {
		max = v;
	}

	public synchronized boolean up() {
		if (current < max) {
			current++;
			return true;
		}

		return false;
	}

	public synchronized void down() {
		current--;
	}

	public int value() {
		return current;
	}

	@Override
	public int emit() {
		return up() ? EMIT : BREAK;
	}

	@Override
	public int finish(FTask task) {
		down();
		if (fr != null) {
			try {
				fr.run();
			} catch (Throwable e) {
			}
		}
		return task.getStatus();
	}

}
