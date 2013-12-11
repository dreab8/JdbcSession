package org.hibernate.resource.transaction.internal;

import javax.transaction.Status;
import javax.transaction.TransactionManager;
import javax.transaction.UserTransaction;

import org.hibernate.TransactionException;
import org.hibernate.engine.transaction.jta.platform.spi.JtaPlatform;
import org.hibernate.resource.jdbc.spi.JdbcSessionImplementor;
import org.hibernate.resource.transaction.PhysicalTransactionDelegate;
import org.hibernate.resource.transaction.SynchronizationRegistry;
import org.hibernate.resource.transaction.TransactionCoordinator;
import org.hibernate.resource.transaction.synchronization.internal.SynchronizationCallbackTarget;

import org.jboss.logging.Logger;

/**
 * An implementation of TransactionCoordinator based on managing a transaction through the JTA API (either TM or UT)
 *
 * @author Steve Ebersole
 */
public class TransactionCoordinatorJtaImpl implements TransactionCoordinator, SynchronizationCallbackTarget {
	private static final Logger log = Logger.getLogger( TransactionCoordinatorJtaImpl.class );

	// NOTE : WORK-IN-PROGRESS

	private final JdbcSessionImplementor jdbcSession;
	private final JtaPlatform jtaPlatform;
	private final boolean autoJoinTransactions;
	private final boolean preferUserTransactions;

	private AbstractPhysicalTransactionDelegate physicalTransactionDelegate;

	private final SynchronizationRegistryStandardImpl synchronizationRegistry = new SynchronizationRegistryStandardImpl();

	private boolean synchronizationRegistered;

	public TransactionCoordinatorJtaImpl(
			JdbcSessionImplementor jdbcSession,
			JtaPlatform jtaPlatform,
			boolean autoJoinTransactions,
			boolean preferUserTransactions) {
		this.jdbcSession = jdbcSession;
		this.jtaPlatform = jtaPlatform;
		this.autoJoinTransactions = autoJoinTransactions;
		this.preferUserTransactions = preferUserTransactions;
	}

	private void prepare() {
		synchronizationRegistered = false;

		if ( autoJoinTransactions ) {
			// todo : implement
		}
	}

	@Override
	public PhysicalTransactionDelegate getPhysicalTransactionDelegate() {
		if ( physicalTransactionDelegate == null ) {
			physicalTransactionDelegate = makePhysicalTransactionDelegate();
		}
		return physicalTransactionDelegate;
	}

	private AbstractPhysicalTransactionDelegate makePhysicalTransactionDelegate() {
		if ( preferUserTransactions ) {
			// The user has requested that we prefer using UserTransaction over TransactionManager
			try {
				final UserTransaction userTransaction = jtaPlatform.retrieveUserTransaction();
				if ( userTransaction != null ) {
					return new UserTransactionDelegateImpl( userTransaction );
				}
			}
			catch (Exception ignore) {
			}

			log.debug( "Could not locate UserTransaction, attempting to use TransactionManager instead" );

			try {
				final TransactionManager transactionManager = jtaPlatform.retrieveTransactionManager();
				if ( transactionManager != null ) {
					return new TransactionManagerDelegateImpl( transactionManager );
				}
			}
			catch (Exception ignore) {
			}
		}
		else {
			// Otherwise, prefer using TransactionManager over UserTransaction
			try {
				final TransactionManager transactionManager = jtaPlatform.retrieveTransactionManager();
				if ( transactionManager != null ) {
					return new TransactionManagerDelegateImpl( transactionManager );
				}
			}
			catch (Exception ignore) {
			}

			log.debug( "Could not locate TransactionManager, attempting to use UserTransaction instead" );

			try {
				final UserTransaction userTransaction = jtaPlatform.retrieveUserTransaction();
				if ( userTransaction != null ) {
					return new UserTransactionDelegateImpl( userTransaction );
				}
			}
			catch (Exception ignore) {
			}
		}

		throw new TransactionException( "Could not locate TransactionManager nor UserTransaction" );
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


	// PhysicalTransactionDelegate ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	/**
	 * Delegate for coordinating with the JTA TransactionManager
	 */
	public class TransactionManagerDelegateImpl extends AbstractPhysicalTransactionDelegate {
		private final TransactionManager transactionManager;

		public TransactionManagerDelegateImpl(TransactionManager transactionManager) {
			super();
			this.transactionManager = transactionManager;
		}

		@Override
		protected void doBegin() {
			try {
				transactionManager.begin();
			}
			catch (Exception e) {
				throw new TransactionException( "JTA TransactionManager#begin failed", e );
			}
		}

		@Override
		protected void doCommit() {
			try {
				transactionManager.commit();
			}
			catch (Exception e) {
				throw new TransactionException( "JTA TransactionManager#commit failed", e );
			}
		}

		@Override
		protected void doRollback() {
			try {
				transactionManager.rollback();
			}
			catch (Exception e) {
				throw new TransactionException( "JTA TransactionManager#rollback failed", e );
			}
		}
	}

	/**
	 * Delegate for coordinating with the JTA TransactionManager
	 */
	public class UserTransactionDelegateImpl extends AbstractPhysicalTransactionDelegate {
		private final UserTransaction userTransaction;

		public UserTransactionDelegateImpl(UserTransaction userTransaction) {
			super();
			this.userTransaction = userTransaction;
		}

		@Override
		protected void doBegin() {
			try {
				userTransaction.begin();
			}
			catch (Exception e) {
				throw new TransactionException( "JTA UserTransaction#begin failed", e );
			}
		}

		@Override
		protected void doCommit() {
			try {
				userTransaction.commit();
			}
			catch (Exception e) {
				throw new TransactionException( "JTA UserTransaction#commit failed", e );
			}
		}

		@Override
		protected void doRollback() {
			try {
				userTransaction.rollback();
			}
			catch (Exception e) {
				throw new TransactionException( "JTA UserTransaction#rollback failed", e );
			}
		}
	}
}
