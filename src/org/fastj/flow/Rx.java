package org.fastj.flow;

import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.function.Function;

public final class Rx {

	private static ThreadLocal<Flow> flowOfThread = new ThreadLocal<>();

	private static final HashMap<String, Latch> IL_LATCHS = new HashMap<>();

	private Rx() {
	}

	public static boolean run() {
		Flow f = flowOfThread.get();
		flowOfThread.remove();
		if (f != null) {
			boolean r = f.sync();
			return r;
		}
		return false;
	}

	public static boolean run(Callback c) {
		Flow f = flowOfThread.get();
		flowOfThread.remove();
		if (f != null) {
			boolean r = f.sync(c);
			return r;
		}
		return false;
	}

	public static boolean run(int timeout) {
		Flow f = flowOfThread.get();
		flowOfThread.remove();
		if (f != null) {
			boolean r = f.sync(null, timeout);
			return r;
		}
		return false;
	}

	public static boolean run(Callback c, int timeout) {
		Flow f = flowOfThread.get();
		flowOfThread.remove();
		if (f != null) {
			boolean r = f.sync(c, timeout);
			return r;
		}
		return false;
	}

	public static void asyncRun(Callback c) {
		Flow f = flowOfThread.get();
		flowOfThread.remove();
		if (f != null) {
			f.async(c);
		}
	}

	public static void asyncRun(Callback c, int timeout) {
		Flow f = flowOfThread.get();
		flowOfThread.remove();
		if (f != null) {
			f.async(c, timeout);
		}
	}

	public static <T> Val<T> get(Callable<T> task) {
		return get((String) null, task, (String) null);
	}

	static <T> Val<T> get(String waitPointer, Callable<T> task) {
		return get(waitPointer, task, (String) null);
	}

	public static <T> Val<T> get(Callable<T> task, String joinPointer) {
		return get((String) null, task, joinPointer);
	}

	public static <T, K> Val<K> get(Function<T, K> task, T t) {
		return get(() -> {
			return task.apply(t);
		});
	}

	public static <T, K> Val<K> get(String waitPointer, Function<T, K> task, T t) {
		return get(waitPointer, () -> {
			return task.apply(t);
		});
	}

	public static <T, K> Val<K> get(Function<T, K> task, T t, String joinPointer) {
		return get(() -> {
			return task.apply(t);
		}, joinPointer);
	}

	public static <T> Val<T> get(String waitPointer, Callable<T> task, String joinPointer) {
		Flow lf = localFlow();
		Val<T> val = new Val<>();
		Line line = lf.line();

		if (waitPointer != null) {
			line.wait(waitPointer);
		}

		line.add(task, val);

		if (joinPointer != null) {
			line.join(joinPointer);
		}
		return val;
	}

	public static void run(Collection<Runnable> tasks, int thread) {
		run(tasks, thread, Flow.TIMEOUT);
	}

	public static void run(Collection<Runnable> tasks, int thread, int timeout) {
		Flow f = Flow.get();

		Latch l = Latch.of(thread);
		for (Runnable r : tasks) {
			f.addTask(r, l);
		}

		f.sync(null, timeout);
	}

	public static void asyncRun(Collection<Runnable> tasks, int thread, int timeout, Callback callback) {
		Flow f = Flow.get();

		Latch l = Latch.of(thread);
		for (Runnable r : tasks) {
			f.addTask(r, l);
		}

		f.async(callback, timeout);
	}

	public static void asyncRun(Runnable task, int timeout) {
		asyncRun(task, timeout, Callback.DEFAULT);
	}

	public static void asyncRun(Runnable task, int timeout, Callback c) {
		Flow f = Flow.get();
		f.addTask(task, null);
		f.async(c != null ? c : Callback.DEFAULT, timeout);
	}

	public static void asyncInLine(Runnable task, String line, int timeout) {
		asyncInLine(task, line, timeout, Callback.DEFAULT);
	}

	public static void asyncInLine(Runnable task, String line, int timeout, Callback c) {
		Flow f = Flow.get();
		f.addTask(task, getInLineLatch(line));
		f.async(c != null ? c : Callback.DEFAULT, timeout);
	}

	public static void asyncStrictInLine(Runnable task, String line, int timeout) {
		asyncStrictInLine(task, line, timeout, Callback.DEFAULT);
	}

	public static void asyncStrictInLine(Runnable task, String line, int timeout, Callback c) {
		Flow f = Flow.get();
		f.addTask(task, strictInLineLatch(line, f));
		f.async(c != null ? c : Callback.DEFAULT, timeout);
	}

	public static void schedule(int total, int cpl, int interval, TFactory tf, Callback callback, int timeout) {
		Flow f = Flow.get();
		f.add(new ScheduleTask(f, total, cpl, interval, tf));
		int tm = (total / cpl + 1) * interval + 15000;
		tm = tm > timeout && timeout != 0 ? tm : timeout;
		f.sync(callback, tm);
	}
	
	public static void schedule(int latch, TFactory tf, Callback callback, int timeout) {
		Flow f = Flow.get();
		f.add(new LatchScheduleTask(f, latch, tf));
		f.sync(callback, timeout);
	}
	
	public static void asyncSchedule(int latch, TFactory tf, Callback callback, int timeout) {
		Flow f = Flow.get();
		f.add(new LatchScheduleTask(f, latch, tf));
		f.async(callback, timeout);
	}

	public static void asyncSchedule(int total, int cpl, int interval, TFactory tf, Callback callback, int timeout) {
		Flow f = Flow.get();
		f.add(new ScheduleTask(f, total, cpl, interval, tf));
		int tm = (total / cpl + 1) * interval + 15000;
		tm = tm > timeout && timeout != 0 ? tm : timeout;
		f.async(callback, tm);
	}

	private static Emiter getInLineLatch(String line) {
		synchronized (IL_LATCHS) {
			Latch l = IL_LATCHS.get(line);
			if (l != null) {
				return l;
			}
			l = new Latch(1);
			IL_LATCHS.put(line, l);
			return l;
		}
	}

	private static Emiter strictInLineLatch(String line, Flow f) {
		synchronized (IL_LATCHS) {
			Latch l = IL_LATCHS.get(line);
			if (l != null) {
				return new ILLatch(l, f);
			}
			l = new Latch(1);
			IL_LATCHS.put(line, l);
			return new ILLatch(l, f);
		}
	}

	private static Flow localFlow() {
		Flow f = flowOfThread.get();
		if (f == null) {
			f = Flow.get();
			flowOfThread.set(f);
			return f;
		}
		return f;
	}

}
