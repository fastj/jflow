package org.fastj.flow;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

public class ILLatch implements Emiter {

	private static final Map<Latch, Holder> HOLDERS = new ConcurrentHashMap<>();
	private final Latch latch;
	private final long sn;
	private volatile Holder holder;

	public ILLatch(Latch latch, Flow f) {
		this.latch = latch;
		Holder h;
		synchronized (HOLDERS) {
			h = HOLDERS.get(latch);
			if (h == null) {
				h = new Holder();
				HOLDERS.put(latch, h);
			}
		}

		holder = h;
		holder.flows.add(f);
		sn = h.sn.getAndIncrement();
	}

	@Override
	public int emit() {
		return sn <= holder.current.longValue() && latch.up() ? EMIT : BREAK;
	}

	@Override
	public int finish(FTask task) {
		int status = task.getStatus();
		holder.flows.remove(task.flow());
		holder.current.incrementAndGet();
		latch.finish(task);
		Flow.schedule(() -> holder.flows.forEach(f -> f.loop()));
		return status;
	}

	static class Holder {
		AtomicLong sn = new AtomicLong(0);
		AtomicLong current = new AtomicLong(0);
		Queue<Flow> flows = new ConcurrentLinkedQueue<>();
	}
}
