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
import org.hibernate.resource.transaction.synchronization.internal.RegisteredSynchronization;
import org.hibernate.resource.transaction.synchronization.internal.SynchronizationCallbackCoordinatorNonTrackingImpl;
import org.hibernate.resource.transaction.synchronization.internal.SynchronizationCallbackCoordinatorTrackingImpl;
import org.hibernate.resource.transaction.synchronization.internal.SynchronizationCallbackTarget;
import org.hibernate.resource.transaction.synchronization.spi.SynchronizationCallbackCoordinator;

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
	private final boolean performJtaThreadTracking;

	private boolean synchronizationRegistered;
	private SynchronizationCallbackCoordinator callbackCoordinator;
	private AbstractPhysicalTransactionDelegate physicalTransactionDelegate;

	private final SynchronizationRegistryStandardImpl synchronizationRegistry = new SynchronizationRegistryStandardImpl();


	public TransactionCoordinatorJtaImpl(
			JdbcSessionImplementor jdbcSession,
			JtaPlatform jtaPlatform,
			boolean autoJoinTransactions,
			boolean preferUserTransactions,
			boolean performJtaThreadTracking) {
		this.jdbcSession = jdbcSession;
		this.jtaPlatform = jtaPlatform;
		this.autoJoinTransactions = autoJoinTransactions;
		this.preferUserTransactions = preferUserTransactions;
		this.performJtaThreadTracking = performJtaThreadTracking;

		synchronizationRegistered = false;

		pulse();
	}

	public SynchronizationCallbackCoordinator getSynchronizationCallbackCoordinator() {
		if ( callbackCoordinator == null ) {
			callbackCoordinator = performJtaThreadTracking
					? new SynchronizationCallbackCoordinatorTrackingImpl( this )
					: new SynchronizationCallbackCoordinatorNonTrackingImpl( this );
		}
		return callbackCoordinator;
	}

	@Override
	public void pulse() {
		if ( !autoJoinTransactions ) {
			return;
		}

		if ( synchronizationRegistered ) {
			return;
		}

		// Can we resister a synchronization according to the JtaPlatform?
		if ( !jtaPlatform.canRegisterSynchronization() ) {
			log.trace( "JTA platform says we cannot currently resister synchronization; skipping" );
			return;
		}

		joinJtaTransaction();
	}

	private void joinJtaTransaction() {
		if ( synchronizationRegistered ) {
			throw new TransactionException( "Hibernate RegisteredSynchronization is already registered for this coordinator" );
		}

		jtaPlatform.registerSynchronization( new RegisteredSynchronization( getSynchronizationCallbackCoordinator() ) );
		getSynchronizationCallbackCoordinator().synchronizationRegistered();
		synchronizationRegistered = true;
		log.debug( "Hibernate RegisteredSynchronization successfully registered with JTA platform" );
	}

	@Override
	public void explicitJoin() {
		joinJtaTransaction();
	}

	/**
	 * Is the RegisteredSynchronization used by Hibernate for unified JTA Synchronization callbacks registered for this
	 * coordinator?
	 *
	 * @return {@code true} indicates that a RegisteredSynchronization is currently registered for this coordinator;
	 * {@code false} indicates it is not (yet) registered.
	 */
	public boolean isSynchronizationRegistered() {
		return synchronizationRegistered;
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
				log.debug( "Error attempting to use UserTransaction for PhysicalTransactionDelegate : " + ignore.getMessage() );
			}

			log.debug( "Could not locate UserTransaction, attempting to use TransactionManager instead" );

			try {
				final TransactionManager transactionManager = jtaPlatform.retrieveTransactionManager();
				if ( transactionManager != null ) {
					return new TransactionManagerDelegateImpl( transactionManager );
				}
			}
			catch (Exception ignore) {
				log.debug( "Error attempting to use TransactionManager for PhysicalTransactionDelegate : " + ignore.getMessage() );
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
				log.debug( "Error attempting to use TransactionManager for PhysicalTransactionDelegate : " + ignore.getMessage() );
			}

			log.debug( "Could not locate TransactionManager, attempting to use UserTransaction instead" );

			try {
				final UserTransaction userTransaction = jtaPlatform.retrieveUserTransaction();
				if ( userTransaction != null ) {
					return new UserTransactionDelegateImpl( userTransaction );
				}
			}
			catch (Exception ignore) {
				log.debug( "Error attempting to use UserTransaction for PhysicalTransactionDelegate : " + ignore.getMessage() );
			}
		}

		throw new TransactionException( "Could not locate TransactionManager nor UserTransaction" );
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

		synchronizationRegistered = false;
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
				log.trace( "Calling TransactionManager#begin" );
				transactionManager.begin();
				log.trace( "Called TransactionManager#begin" );
			}
			catch (Exception e) {
				throw new TransactionException( "JTA TransactionManager#begin failed", e );
			}
		}

		@Override
		protected void afterBegin() {
			super.afterBegin();
			TransactionCoordinatorJtaImpl.this.joinJtaTransaction();
		}

		@Override
		protected void doCommit() {
			try {
				log.trace( "Calling TransactionManager#commit" );
				transactionManager.commit();
				log.trace( "Called TransactionManager#commit" );
			}
			catch (Exception e) {
				throw new TransactionException( "JTA TransactionManager#commit failed", e );
			}
		}

		@Override
		protected void doRollback() {
			try {
				log.trace( "Calling TransactionManager#rollback" );
				transactionManager.rollback();
				log.trace( "Called TransactionManager#rollback" );
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
				log.trace( "Calling UserTransaction#begin" );
				userTransaction.begin();
				log.trace( "Called UserTransaction#begin" );
			}
			catch (Exception e) {
				throw new TransactionException( "JTA UserTransaction#begin failed", e );
			}
		}

		@Override
		protected void afterBegin() {
			super.afterBegin();
			TransactionCoordinatorJtaImpl.this.joinJtaTransaction();
		}

		@Override
		protected void doCommit() {
			try {
				log.trace( "Calling UserTransaction#commit" );
				userTransaction.commit();
				log.trace( "Called UserTransaction#commit" );
			}
			catch (Exception e) {
				throw new TransactionException( "JTA UserTransaction#commit failed", e );
			}
		}

		@Override
		protected void doRollback() {
			try {
				log.trace( "Calling UserTransaction#rollback" );
				userTransaction.rollback();
				log.trace( "Called UserTransaction#rollback" );
			}
			catch (Exception e) {
				throw new TransactionException( "JTA UserTransaction#rollback failed", e );
			}
		}
	}
}
