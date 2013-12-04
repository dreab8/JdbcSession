package org.hibernate.resource.transaction.synchronization.internal;

/**
 * @author Steve Ebersole
 */
public interface SynchronizationCallbackTarget {
	boolean isActive();

	void beforeCompletion();

	void afterCompletion(boolean successful);
}
