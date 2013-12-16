package org.hibernate.resource.transaction.internal;

import javax.transaction.Status;
import javax.transaction.TransactionManager;
import javax.transaction.UserTransaction;

import org.hibernate.TransactionException;
import org.hibernate.engine.transaction.jta.platform.spi.JtaPlatform;
import org.hibernate.resource.transaction.PhysicalTransactionDelegate;
import org.hibernate.resource.transaction.SynchronizationRegistry;
import org.hibernate.resource.transaction.TransactionCoordinator;
import org.hibernate.resource.transaction.TransactionCoordinatorOwner;
import org.hibernate.resource.transaction.synchronization.internal.RegisteredSynchronization;
import org.hibernate.resource.transaction.synchronization.internal.SynchronizationCallbackCoordinatorNonTrackingImpl;
import org.hibernate.resource.transaction.synchronization.internal.SynchronizationCallbackCoordinatorTrackingImpl;
import org.hibernate.resource.transaction.synchronization.internal.SynchronizationCallbackTarget;
import org.hibernate.resource.transaction.synchronization.spi.SynchronizationCallbackCoordinator;

import org.jboss.logging.Logger;

import static org.hibernate.internal.CoreLogging.logger;

/**
 * An implementation of TransactionCoordinator based on managing a transaction through the JTA API (either TM or UT)
 *
 * @author Steve Ebersole
 */
public class TransactionCoordinatorJtaImpl implements TransactionCoordinator, SynchronizationCallbackTarget {
	private static final Logger log = logger( TransactionCoordinatorJtaImpl.class );

	private final TransactionCoordinatorOwner owner;
	private final JtaPlatform jtaPlatform;
	private final boolean autoJoinTransactions;
	private final boolean preferUserTransactions;
	private final boolean performJtaThreadTracking;

	private boolean synchronizationRegistered;
	private SynchronizationCallbackCoordinator callbackCoordinator;
	private AbstractPhysicalTransactionDelegate physicalTransactionDelegate;

	private final SynchronizationRegistryStandardImpl synchronizationRegistry = new SynchronizationRegistryStandardImpl();

	/**
	 * Construct a TransactionCoordinatorJtaImpl instance.  package-protected to ensure access goes through
	 * builder.
	 *
	 * @param owner The owner
	 * @param jtaPlatform The JtaPlatform to use
	 * @param autoJoinTransactions Should JTA transactions be auto-joined?  Or should we wait for explicit join calls?
	 * @param preferUserTransactions Should we prefer using UserTransaction, as opposed to TransactionManager?
	 * @param performJtaThreadTracking Should we perform thread tracking?
	 */
	TransactionCoordinatorJtaImpl(
			TransactionCoordinatorOwner owner,
			JtaPlatform jtaPlatform,
			boolean autoJoinTransactions,
			boolean preferUserTransactions,
			boolean performJtaThreadTracking) {
		this.owner = owner;
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

	/**
	 * Join to the JTA transaction.  Note that the underlying meaning of joining in JTA environments is to register the
	 * RegisteredSynchronization with the JTA system
	 */
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
		if ( synchronizationRegistered ) {
			log.debug( "JTA transaction was already joined (RegisteredSynchronization already registered)" );
			return;
		}

		joinJtaTransaction();
	}

	@Override
	public boolean isJoined() {
		return synchronizationRegistered;
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
		AbstractPhysicalTransactionDelegate delegate;

		if ( preferUserTransactions ) {
			delegate = makeUserTransactionDelegate();

			if ( delegate == null ) {
				log.debug( "Unable to access UserTransaction, attempting to use TransactionManager instead" );
				delegate = makeTransactionManagerDelegate();
			}
		}
		else {
			delegate = makeTransactionManagerDelegate();

			if ( delegate == null ) {
				log.debug( "Unable to access TransactionManager, attempting to use UserTransaction instead" );
				delegate = makeUserTransactionDelegate();
			}
		}

		if ( delegate == null ) {
			throw new JtaPlatformInaccessibleException(
					"Unable to access TransactionManager or UserTransaction to make physical transaction delegate"
			);
		}

		return delegate;
	}

	private UserTransactionDelegateImpl makeUserTransactionDelegate() {
		try {
			final UserTransaction userTransaction = jtaPlatform.retrieveUserTransaction();
			if ( userTransaction == null ) {
				log.debug( "JtaPlatform#retrieveUserTransaction returned null" );
			}
			else {
				return new UserTransactionDelegateImpl( userTransaction );
			}
		}
		catch (Exception ignore) {
			log.debugf( "JtaPlatform#retrieveUserTransaction threw an exception [%s]", ignore.getMessage() );
		}

		return null;
	}

	private TransactionManagerDelegateImpl makeTransactionManagerDelegate() {
		try {
			final TransactionManager transactionManager = jtaPlatform.retrieveTransactionManager();
			if ( transactionManager == null ) {
				log.debug( "JtaPlatform#retrieveTransactionManager returned null" );
			}
			else {
				return new TransactionManagerDelegateImpl( transactionManager );
			}
		}
		catch (Exception ignore) {
			log.debugf( "JtaPlatform#retrieveTransactionManager threw an exception [%s]", ignore.getMessage() );
		}

		return null;
	}

	@Override
	public SynchronizationRegistry getLocalSynchronizations() {
		return synchronizationRegistry;
	}


	// SynchronizationCallbackTarget ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	@Override
	public boolean isActive() {
		return owner.isActive();
	}

	@Override
	public void beforeCompletion() {
		owner.beforeTransactionCompletion();
		synchronizationRegistry.notifySynchronizationsBeforeTransactionCompletion();
	}

	@Override
	public void afterCompletion(boolean successful) {
		final int statusToSend =  successful ? Status.STATUS_COMMITTED : Status.STATUS_UNKNOWN;
		synchronizationRegistry.notifySynchronizationsAfterTransactionCompletion( statusToSend );

		owner.afterTransactionCompletion( successful );

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
