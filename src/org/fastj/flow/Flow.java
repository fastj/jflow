package org.fastj.flow;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class Flow {

	// 180 seconds
	public static int TIMEOUT = 180000;

	public static final int COMPUTE_THREAD = computeThread();

	private static final ExecutorService IO_POOL = Executors.newCachedThreadPool(new ThFactory("jflow.task.worker", false));

	private static final ExecutorService SCHEDULE_POOL = new ThreadPoolExecutor(2, 2, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),
			new ThFactory("jflow.schedule.worker", true, 6));

	private static final ExecutorService COMPUTE_POOL = new ThreadPoolExecutor(COMPUTE_THREAD, COMPUTE_THREAD, 60L, TimeUnit.SECONDS,
			new LinkedBlockingQueue<Runnable>(), new ThFactory("jflow.compute.worker", true, 9));

	private static final Object NOTIFIER = new Object();

	private static final Map<AtomicInteger, Runnable> MONITORS = new ConcurrentHashMap<>(128);

	private Map<String, FTask> tasks = new ConcurrentSkipListMap<>();

	private Map<String, FTask> finishTasks = new ConcurrentHashMap<>(32);

	private Map<String, JoinPointer> joinPointers = new ConcurrentHashMap<>(32);

	private Map<String, Object> context = new ConcurrentHashMap<>(32);

	private AtomicInteger notifyCnt = new AtomicInteger(0);

	private Callback callback = Callback.DEFAULT;

	private AtomicInteger status = new AtomicInteger(FTask.INIT);

	private long startTime = 0L;

	private int timeout = TIMEOUT;

	private List<Throwable> errors = new ArrayList<>();

	private Runnable looper = new Looper();

	private Runnable tmoutChker = new TimeoutChecker();

	private List<Flow> subflow = new ArrayList<>();

	static {
		Thread mth = new Thread(new Monitor());
		mth.setDaemon(true);
		mth.setName("jflow.monitor");
		mth.start();
	}

	public static Flow get() {
		return new Flow();
	}

	private Flow() {
	}

	public boolean sync() {
		return sync(null);
	}

	public boolean sync(Callback cb) {
		return sync(cb, TIMEOUT);
	}

	public boolean sync(Callback cb, int timeout) {
		this.startTime = System.currentTimeMillis();
		this.status.compareAndSet(FTask.INIT, FTask.STARTED);
		this.timeout = timeout <= 0 ? Integer.MAX_VALUE : timeout;
		this.notifyCnt.incrementAndGet();
		long end = System.currentTimeMillis() + this.timeout;
		this.callback = cb == null ? Callback.DEFAULT : cb;

		// nothing
		if (tasks.isEmpty() && subflow.isEmpty()) {
			status.set(FTask.DONE);
			callback();
			return true;
		}

		for (Flow sub : subflow) {
			sub.async(null, timeout);
		}
		SCHEDULE_POOL.submit(looper);
		while (System.currentTimeMillis() < end && status.intValue() < FTask.DONE) {
			synchronized (notifyCnt) {
				try {
					notifyCnt.wait(1);
				} catch (InterruptedException e) {
				}
			}
		}

		onTaskFail(null, null, FTask.TIMEOUT);

		return status.intValue() == FTask.DONE;
	}

	public void async(Callback callback) {
		async(callback, TIMEOUT);
	}

	public void async(Callback callback, int timeout) {
		this.startTime = System.currentTimeMillis();
		this.status.compareAndSet(FTask.INIT, FTask.STARTED);
		this.callback = callback;
		this.timeout = timeout <= 0 ? Integer.MAX_VALUE : timeout;
		notifyCnt.incrementAndGet();

		// nothing
		if (tasks.isEmpty() && subflow.isEmpty()) {
			status.set(FTask.DONE);
			callback();
			return;
		}

		MONITORS.put(notifyCnt, tmoutChker);
		for (Flow sub : subflow) {
			sub.async(null, timeout);
		}
		SCHEDULE_POOL.submit(looper);
	}

	public Line line() {
		return new Line(this);
	}

	public void addTask(Runnable task, Emiter emitor) {
		FTask t = new FTask(this, emitor) {
			public void run() {
				try {
					task.run();
					done();
				} catch (Throwable e) {
					error(e);
				}
			}
		};

		add(t);
	}

	public <T> void addVar(String key, T v) {
		synchronized (context) {
			context.put(key, v);
		}
	}

	@SuppressWarnings("unchecked")
	public <T> T getVar(String key) {
		synchronized (context) {
			return (T) context.get(key);
		}
	}

	public int getTaskCount() {
		return tasks.size() + finishTasks.size();
	}

	public boolean isDone() {
		return this.status.intValue() > FTask.STARTED;
	}

	public Throwable getError() {
		return errors.isEmpty() ? null : errors.get(0);
	}

	public List<Throwable> getErrors() {
		return errors;
	}

	public long getStartTime() {
		return startTime;
	}

	private void emits() {
		Iterator<Entry<String, FTask>> it = tasks.entrySet().iterator();
		for (; it.hasNext();) {
			Entry<String, FTask> e = it.next();
			int erlt = emit(e.getValue());
			if (erlt == Emiter.BREAK) {
				break;
			}
		}
	}

	private int emit(FTask t) {
		// Flow is done or error
		if (status.intValue() > FTask.STARTED) {
			return Emiter.BREAK;
		}

		Emiter emiter = t.getEmiter();
		synchronized (t) {
			if (t.canEmit()) {
				int emit = emiter == null ? Emiter.EMIT : emiter.emit();
				if (emit == Emiter.EMIT) {
					if (t instanceof JoinPointer || t instanceof AdaptorTask) {
						t.start(null);
						t.run();
					} else if (t instanceof ComputeTask) {
						try {
							Future<?> f = COMPUTE_POOL.submit(t);
							t.start(f);
						} catch (RejectedExecutionException e) {
							return Emiter.BREAK;
						}
					} else {
						try {
							Future<?> f = IO_POOL.submit(t);
							t.start(f);
						} catch (RejectedExecutionException e) {
							return Emiter.BREAK;
						}
					}
				}
				return emit;
			}
		}

		return Emiter.NEXT;
	}

	synchronized void onTaskDone(FTask task) {
		finishTasks.put(task.name(), task);
		tasks.remove(task.name());

		if (tasks.isEmpty()) {
			if (this.status.compareAndSet(FTask.STARTED, FTask.DONE)) {
				callback();
			}
		}

		notifyCnt.incrementAndGet();
		SCHEDULE_POOL.submit(looper);

		synchronized (notifyCnt) {
			notifyCnt.notifyAll();
		}
	}

	synchronized void onTaskFail(FTask task, Throwable t, int status) {
		if (task != null) {
			finishTasks.put(task.name(), task);
			tasks.remove(task.name());
		}

		if (t != null) {
			this.errors.add(t);
		}

		if (this.status.compareAndSet(FTask.STARTED, status)) {
			callback();
		}

		synchronized (notifyCnt) {
			notifyCnt.notifyAll();
		}
	}

	private void callback() {
		MONITORS.remove(notifyCnt);
		synchronized (notifyCnt) {
			notifyCnt.notifyAll();
			if (callback != null) {
				try {
					callback.run(this);
				} catch (Throwable e) {
					e.printStackTrace();
				} finally {
					callback = null;
					tasks.forEach((k, t) -> {
						t.cancel();
					});
				}
			}
		}
	}

	public int status() {
		return status.intValue();
	}

	boolean allDepsDone(List<String> deps) {
		boolean done = true;
		synchronized (deps) {
			for (String dep : deps) {
				FTask t;
				done = done && ((finishTasks.containsKey(dep) && finishTasks.get(dep).isDone()) || ((t = tasks.get(dep)) != null && t.isDone()));
				if (!done) {
					return done;
				}
			}
		}

		return done;
	}

	public boolean isOK() {
		return this.status.intValue() == FTask.DONE;
	}

	public boolean isError() {
		return this.status.intValue() > FTask.DONE;
	}

	void loop() {
		SCHEDULE_POOL.submit(looper);
	}

	void add(FTask task) {
		tasks.put(task.name(), task);
	}

	JoinPointer getJoinPointer(String name) {
		JoinPointer jp = joinPointers.get(name);
		if (jp == null) {
			jp = new JoinPointer(this, name);
			joinPointers.put(name, jp);
		}

		return jp;
	}

	private static final class Monitor implements Runnable {
		public void run() {
			while (true) {
				synchronized (NOTIFIER) {
					try {
						NOTIFIER.wait(10);
					} catch (Throwable e) {
					}
				}
				for (Entry<AtomicInteger, Runnable> e : MONITORS.entrySet()) {
					SCHEDULE_POOL.execute(new Runnable() {
						public void run() {
							e.getValue().run();
						}
					});
				}
			}
		}
	}

	private final class TimeoutChecker implements Runnable {
		public void run() {
			timeout();
		}
	}

	private final class Looper implements Runnable {
		public void run() {
			emits();
		}
	}

	private void timeout() {
		if (startTime > 0 && (System.currentTimeMillis() - startTime) > timeout) {
			onTaskFail(null, new TimeoutException(), FTask.TIMEOUT);
		}
	}

	static void schedule(Runnable r) {
		SCHEDULE_POOL.submit(r);
	}

	private final static class ThFactory implements ThreadFactory {

		private static final AtomicInteger COUNT = new AtomicInteger();

		private final String name;
		private final boolean daemon;
		private final int priority;

		ThFactory(String name, boolean daemon) {
			this.name = name;
			this.daemon = daemon;
			this.priority = 10;
		}

		ThFactory(String name, boolean daemon, int priority) {
			this.name = name;
			this.daemon = daemon;
			this.priority = priority;
		}

		@Override
		public Thread newThread(Runnable r) {
			Thread t = new Thread(r, name + "-" + COUNT.incrementAndGet());
			t.setDaemon(daemon);
			t.setPriority(priority);
			return t;
		}

	}

	static class ComputeTask extends FTask {

		final Runnable adaptor;

		public ComputeTask(Flow flow, Runnable r) {
			super(flow);
			this.adaptor = r;
		}

		public void run() {
			try {
				adaptor.run();
				done();
			} catch (Throwable e) {
				error(e);
			}
		}
	}

	static class AdaptorTask extends FTask {

		final Runnable adaptor;

		public AdaptorTask(Flow flow, Runnable r) {
			super(flow);
			this.adaptor = r;
		}

		public void run() {
			try {
				adaptor.run();
				done();
			} catch (Throwable e) {
				error(e);
			}
		}
	}

	static int computeThread() {
		String prop = System.getProperty("rxflow.compute.thread");
		int cpu = Runtime.getRuntime().availableProcessors();
		int ccpu = cpu > 8 ? cpu - 4 : cpu <= 2 ? cpu : cpu - 1;
		int set = prop == null ? 0 : Integer.valueOf(prop);
		return set > 0 && set <= cpu ? set : ccpu;
	}

	void innercheck() {

		System.out.println("-----------------------------Flow status : " + status);

		if (status.intValue() < FTask.DONE) {
			System.out.println("notify ： " + MONITORS.containsKey(notifyCnt) + "  " + MONITORS.size() + "   nv: " + notifyCnt.intValue());
			MONITORS.remove(notifyCnt);
		}

		tasks.forEach((k, v) -> {
			System.out.println("-----unfinish----Task: " + v.name() + " status: " + v.getStatus() + " st: " + v.getStartTime() + " et: " + v.getEndTime());
			System.out.println("Deps: " + v.deps());
		});

		System.out.println("-----sub");
		subflow.forEach(f -> f.innercheck());
	}
}
