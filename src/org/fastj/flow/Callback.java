package org.fastj.flow;

public interface Callback {
	void run(Flow f);

	static Callback DEFAULT = new Callback() {
		@Override
		public void run(Flow f) {
			if (f.isError()) {
				f.innercheck();
				if (f.getError() != null) {
					f.getError().printStackTrace();
				}
			} else {
				System.out.println("Flow finished. Takes " + (System.currentTimeMillis() - f.getStartTime()));
			}
		}
	};

}
