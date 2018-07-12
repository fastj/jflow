package org.fastj.flow;

import java.util.ArrayList;
import java.util.List;

public class Val<T> {

	private final List<Runnable> callbacks = new ArrayList<>();

	private T entity;

	private Throwable error = null;

	private long startTime = System.currentTimeMillis();

	private long endTime = 0l;

	private Flow flow;

	private String relatedTask;

	public void error(Throwable err) {
		this.error = err;
		synchronized (callbacks) {
			this.endTime = System.currentTimeMillis();
			callbacks.notifyAll();
			for (Runnable r : callbacks) {
				try {
					r.run();
				} catch (Throwable e) {
					e.printStackTrace();
				}
			}
		}
	}

	public void set(T entity) {
		this.entity = entity;
		synchronized (callbacks) {
			this.endTime = System.currentTimeMillis();
			callbacks.notifyAll();
			for (Runnable r : callbacks) {
				try {
					r.run();
				} catch (Throwable e) {
					e.printStackTrace();
				}
			}
		}
	}

	public T get() {
		return get(0);
	}

	public T get(int timeout) {
		if (endTime >= startTime) {
			if (error != null) {
				throw new RxException(error);
			}
			return entity;
		}

		long wend = timeout <= 0 ? 0L : System.currentTimeMillis() + timeout;

		while (wend == 0 || wend > System.currentTimeMillis()) {
			synchronized (callbacks) {
				try {
					callbacks.wait(100);
				} catch (InterruptedException e) {
				}
				if (endTime >= startTime) {
					if (error != null) {
						throw new RxException(error);
					}
					return entity;
				}
			}
		}

		if (endTime >= startTime) {
			if (error != null) {
				throw new RxException(error);
			}
			return entity;
		} else {
			throw new RxException("Timeout");
		}
	}

	public Throwable error() {
		return error;
	}

	public void start() {
		startTime = System.currentTimeMillis();
	}

	public long getEndTime() {
		return endTime;
	}

	public long getStartTime() {
		return startTime;
	}

	public void addCallback(Runnable r) {
		synchronized (callbacks) {
			callbacks.add(r);
			if (endTime >= startTime) {
				r.run();
			}
		}
	}

	public Flow getFlow() {
		return flow;
	}

	protected void setFlow(Flow flow) {
		this.flow = flow;
	}

	public String getRelatedTask() {
		return relatedTask;
	}

	public void setRelatedTask(String relatedTask) {
		this.relatedTask = relatedTask;
	}
}
