package org.hibernate.resource2.transaction.synchronization.internal;

/**
 * @author Steve Ebersole
 */
public interface SynchronizationCallbackInflow {
	boolean isActive();

	void beforeCompletion();

	void afterCompletion(boolean successful);
}
