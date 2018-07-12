package org.fastj.flow;

public class RxException extends RuntimeException {
	private static final long serialVersionUID = 2616637740822436671L;

	public RxException() {
		super();
	}

	public RxException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public RxException(String message, Throwable cause) {
		super(message, cause);
	}

	public RxException(String message) {
		super(message);
	}

	public RxException(Throwable cause) {
		super(cause);
	}
}
