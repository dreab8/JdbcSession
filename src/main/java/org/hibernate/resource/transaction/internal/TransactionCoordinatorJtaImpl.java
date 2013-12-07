package org.hibernate.resource.transaction.internal;

import javax.transaction.Status;

import org.hibernate.engine.transaction.jta.platform.spi.JtaPlatform;
import org.hibernate.resource.jdbc.spi.JdbcSessionImplementor;
import org.hibernate.resource.transaction.PhysicalTransactionDelegate;
import org.hibernate.resource.transaction.SynchronizationRegistry;
import org.hibernate.resource.transaction.TransactionCoordinator;
import org.hibernate.resource.transaction.synchronization.internal.SynchronizationCallbackTarget;

/**
 * An implementation of TransactionCoordinator based on managing a transaction through the JTA API (either TM or UT)
 *
 * @author Steve Ebersole
 */
public class TransactionCoordinatorJtaImpl implements TransactionCoordinator, SynchronizationCallbackTarget {

	// NOTE : WORK-IN-PROGRESS

	private final JdbcSessionImplementor jdbcSession;
	private final JtaPlatform jtaPlatform;
	private final boolean autoJoinTransactions;
	private final SynchronizationRegistryStandardImpl synchronizationRegistry = new SynchronizationRegistryStandardImpl();

	private boolean synchronizationRegistered;

	public TransactionCoordinatorJtaImpl(
			JdbcSessionImplementor jdbcSession,
			JtaPlatform jtaPlatform, boolean autoJoinTransactions) {
		this.jdbcSession = jdbcSession;
		this.jtaPlatform = jtaPlatform;
		this.autoJoinTransactions = autoJoinTransactions;
	}

	private void prepare() {
		synchronizationRegistered = false;

		if ( autoJoinTransactions ) {
			// todo : implement
		}
	}

	@Override
	public PhysicalTransactionDelegate getPhysicalTransactionDelegate() {
		return null;  //To change body of implemented methods use File | Settings | File Templates.
	}

	@Override
	public void pulse() {
		//To change body of implemented methods use File | Settings | File Templates.
	}

	@Override
	public SynchronizationRegistry getLocalSynchronizations() {
		return synchronizationRegistry;
	}


	// SynchronizationCallbackTarget ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	@Override
	public boolean isActive() {
		return jdbcSession.isOpen();
	}

	@Override
	public void beforeCompletion() {
		synchronizationRegistry.notifySynchronizationsBeforeTransactionCompletion();
	}

	@Override
	public void afterCompletion(boolean successful) {
		final int statusToSend =  successful ? Status.STATUS_COMMITTED : Status.STATUS_UNKNOWN;
		synchronizationRegistry.notifySynchronizationsAfterTransactionCompletion( statusToSend );

		prepare();
	}
}
