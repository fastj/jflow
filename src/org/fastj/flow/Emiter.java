package org.fastj.flow;

public interface Emiter {

	int EMIT = 0;

	int NEXT = 1;

	int BREAK = 10;

	int emit();

	int finish(FTask task);

}
