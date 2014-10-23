package com.microsoft.storm.orc;

public abstract class ErrorReporter {
	public abstract void reportError(Throwable error);
}
