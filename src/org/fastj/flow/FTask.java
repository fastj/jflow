package org.fastj.flow;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public abstract class FTask implements Runnable {

	public static final int INIT = 0;
	public static final int STARTED = 1;
	public static final int DONE = 2;
	public static final int ERROR = 3;
	public static final int TIMEOUT = 9;
	public static final int CANCELED = 10;

	private static final AtomicLong ID = new AtomicLong(0);

	private final Flow flow;
	protected Emiter emiter = null;
	private final String name;
	private Throwable error = null;
	private Future<?> future = null;
	private AtomicInteger status = new AtomicInteger(INIT);

	private long startTime = 0L;
	private long endTime = 0L;

	private List<String> deps = new ArrayList<String>();

	public FTask(Flow flow) {
		if (flow == null) {
			throw new IllegalArgumentException("FTask flow is null");
		}
		this.flow = flow;
		this.name = "T-" + ID.incrementAndGet();
	}

	public FTask(Flow flow, String name) {
		if (flow == null) {
			throw new IllegalArgumentException("FTask flow is null");
		}
		if (name == null) {
			throw new IllegalArgumentException("FTask name is null");
		}
		this.flow = flow;
		this.name = name;
	}

	public FTask(Flow flow, Emiter emiter) {
		if (flow == null) {
			throw new IllegalArgumentException("FTask flow is null");
		}
		this.flow = flow;
		this.emiter = emiter;
		this.name = "T-" + ID.incrementAndGet();
	}

	public FTask(Flow flow, String name, Emiter emiter) {
		if (flow == null) {
			throw new IllegalArgumentException("FTask flow is null");
		}
		if (name == null) {
			throw new IllegalArgumentException("FTask name is null");
		}
		this.flow = flow;
		this.name = name;
		this.emiter = emiter;
	}

	public Flow flow() {
		return flow;
	}

	public List<String> deps() {
		return Collections.unmodifiableList(deps);
	}

	Emiter getEmiter() {
		return emiter;
	}

	void depsOn(String taskName) {
		synchronized (deps) {
			deps.add(taskName);
		}
	}

	public String name() {
		return name;
	}

	public abstract void run();

	public boolean canEmit() {
		if (status.intValue() > INIT) {
			return false;
		}

		boolean isDepsOK = allDepsDone();
		return isDepsOK;
	}

	public boolean isDone() {
		return status.intValue() == DONE;
	}

	public boolean isError() {
		return status.intValue() > DONE;
	}

	public boolean isTimeout() {
		return status.intValue() == TIMEOUT;
	}

	public int getStatus() {
		return status.intValue();
	}

	public Throwable getError() {
		return error;
	}

	private boolean allDepsDone() {
		return flow == null || deps.isEmpty() || flow.allDepsDone(deps);
	}

	synchronized void done() {
		if (status.intValue() > STARTED) {
			return;
		}
		setStatus(DONE);
		if (emiter != null) {
			emiter.finish(this);
		}
		endTime = System.currentTimeMillis();
		flow.onTaskDone(this);
	}

	synchronized boolean terminate() {
		return status.intValue() > STARTED;
	}

	void start(Future<?> f) {
		this.future = f;
		status.compareAndSet(INIT, STARTED);
		startTime = System.currentTimeMillis();
	}

	void error(Throwable t) {
		if (status.intValue() > STARTED) {
			return;
		}
		this.error = t;
		setStatus(ERROR);
		if (emiter != null) {
			emiter.finish(this);
		}
		endTime = System.currentTimeMillis();
		flow.onTaskFail(this, t, ERROR);
	}

	synchronized void timeout() {
		if (status.intValue() > STARTED) {
			return;
		}
		setStatus(TIMEOUT);
		if (emiter != null) {
			emiter.finish(this);
		}
		endTime = System.currentTimeMillis();
		flow.onTaskFail(this, new TimeoutException("TMOUT: " + (endTime - startTime)), TIMEOUT);
	}

	synchronized void cancel() {
		if (future != null && !future.isDone() && !future.isCancelled()) {
			future.cancel(true);
		}
		setStatus(CANCELED);
	}

	private boolean setStatus(int status) {
		return this.status.compareAndSet(INIT, status) || this.status.compareAndSet(STARTED, status);
	}

	public int hashCode() {
		return name.hashCode();
	}

	public boolean equals(Object obj) {
		if (obj instanceof FTask) {
			return name.equals(((FTask) obj).name());
		}
		return false;
	}

	public long getStartTime() {
		return startTime;
	}

	public long getEndTime() {
		return endTime;
	}
}
