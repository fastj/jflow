package org.fastj.flow;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.fastj.flow.Flow.AdaptorTask;
import org.fastj.flow.Flow.ComputeTask;

public class Line {

	private static final AtomicLong ID_GEN = new AtomicLong(0);

	private Flow flow = null;

	private FTask last = null;

	Line(Flow flow) {
		this.flow = flow;
	}

	public Flow flow() {
		return flow;
	}

	public Line add(Runnable task) {
		return add(task, null);
	}

	public Line add(Runnable task, Emiter emitor) {
		FTask t = new FTask(this.flow, emitor) {
			public void run() {
				try {
					task.run();
					done();
				} catch (Throwable e) {
					error(e);
				}
			}
		};

		flow.add(t);
		if (last != null) {
			t.depsOn(last.name());
		}
		last = t;
		return this;
	}

	public <T> void add(Callable<T> task, Val<T> val) {
		FTask t = new FTask(this.flow, (Emiter) null) {
			public void run() {
				try {
					T v = task.call();
					val.set(v);
					done();
				} catch (Throwable e) {
					error(e);
					val.error(e);
				}
			}

			@Override
			public void cancel() {
				super.cancel();
				val.error(flow.getError());
			}
		};

		val.setFlow(flow);
		val.setRelatedTask(t.name());

		flow.add(t);
		if (last != null) {
			t.depsOn(last.name());
		}
		last = t;
	}

	public <T, R> Line race(final T entity, OrValueHolder<R> joinPointer, Collection<Function<T, R>> funcs) {

		for (Function<T, R> func : funcs) {
			FTask t = new FTask(this.flow) {
				public void run() {
					try {
						R r = func.apply(entity);
						joinPointer.set(r);
						done();
					} catch (Throwable e) {
						joinPointer.onError(e);
					}
				}
			};

			joinPointer.addRelationTask(t);
			flow.add(t);
			if (last != null) {
				t.depsOn(last.name());
			}
			joinPointer.depsOn(t.name());
		}

		flow.add(joinPointer);
		last = joinPointer;

		return this;
	}

	public <T, R> Line all(final T entity, ArrayValueHolder<R> joinPointer, Collection<Function<T, R>> funcs) {

		AtomicInteger idx = new AtomicInteger(0);
		for (Function<T, R> func : funcs) {
			FTask t = new FTask(this.flow) {
				final int index = idx.getAndIncrement();

				public void run() {
					try {
						R r = func.apply(entity);
						joinPointer.set(index, r);
						done();
					} catch (Throwable e) {
						error(e);
					}
				}
			};

			flow.add(t);
			if (last != null) {
				t.depsOn(last.name());
			}
			joinPointer.depsOn(t.name());
		}

		flow.add(joinPointer);
		last = joinPointer;

		return this;
	}

	public Line join(String name) {
		return join(name, null);
	}

	public Line join(String name, List<String> deps) {
		JoinPointer jp = flow.getJoinPointer(name);
		flow.add(jp);
		if (last != null) {
			jp.depsOn(last.name());
		}
		if (deps != null) {
			for (String dep : deps) {
				jp.depsOn(dep);
			}
		}
		last = jp;
		return this;
	}

	public Line wait(String... names) {
		if (last != null) {
			throw new IllegalArgumentException("Cannot wait(...) twice");
		}

		if (names == null || names.length == 0) {
			return this;
		}

		if (names.length == 1) {
			String name = names[0];
			JoinPointer jp = flow.getJoinPointer(name);
			flow.add(jp);
			last = jp;
		} else {
			String name = "MWA_JP" + ID_GEN.incrementAndGet();
			JoinPointer jp = flow.getJoinPointer(name);
			for (String dep : names) {
				jp.depsOn(dep);
			}
			flow.add(jp);
			last = jp;
		}

		return this;
	}

	Line adaptor(Runnable r) {
		AdaptorTask t = new AdaptorTask(flow, r);
		flow.add(t);
		if (last != null) {
			t.depsOn(last.name());
		}
		last = t;
		return this;
	}

	Line compute(Runnable r) {
		ComputeTask t = new ComputeTask(flow, r);
		flow.add(t);
		if (last != null) {
			t.depsOn(last.name());
		}
		last = t;
		return this;
	}

	static class OrValueHolder<T> extends JoinPointer {
		static final AtomicInteger ID = new AtomicInteger(0);

		List<FTask> rTasks = new ArrayList<>();
		T entity;
		List<Throwable> errors = new ArrayList<>();
		final int raceSize;

		public OrValueHolder(Flow flow, int size) {
			super(flow, "ORVH" + ID.incrementAndGet());
			this.raceSize = size;
		}

		synchronized void set(T entity) {
			if (!isDone() && this.entity == null) {
				this.entity = entity;
				run();
			}
			rTasks.forEach(t -> {
				t.done();
			});
		}

		T get() {
			return entity;
		}

		synchronized void onError(Throwable e) {
			this.errors.add(e);
			if (this.errors.size() == raceSize) {
				error(e);
			}
		}

		void addRelationTask(FTask t) {
			rTasks.add(t);
		}

		@Override
		public boolean canEmit() {
			return false;
		}
	}

	static class ArrayValueHolder<T> extends JoinPointer {
		static final AtomicInteger ID = new AtomicInteger(0);

		private Object[] entitys;
		private List<Throwable> errors = new ArrayList<>();
		private AtomicInteger count = new AtomicInteger(0);

		public ArrayValueHolder(Flow flow, int size) {
			super(flow, "ARRVH" + ID.incrementAndGet());
			entitys = new Object[size];
		}

		synchronized void set(int idx, T entity) {
			if (!isDone()) {
				this.entitys[idx] = entity;
			}
			count.incrementAndGet();
			if (count.intValue() == entitys.length) {
				if (errors.isEmpty()) {
					run();
				} else {
					error(errors.get(0));
				}
			}
		}

		synchronized void error(int idx, Throwable t) {
			errors.add(t);
			count.incrementAndGet();
			error(t);
		}

		@SuppressWarnings("unchecked")
		public T[] get() {
			return (T[]) entitys;
		}

		public boolean isError() {
			return !errors.isEmpty();
		}

		@Override
		public boolean canEmit() {
			return false;
		}
	}
}
