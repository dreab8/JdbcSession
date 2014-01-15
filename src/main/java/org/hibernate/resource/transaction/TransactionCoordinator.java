/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * Copyright (c) 2013, Red Hat Inc. or third-party contributors as
 * indicated by the @author tags or express copyright attribution
 * statements applied by the authors.  All third-party contributions are
 * distributed under license by Red Hat Inc.
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, write to:
 * Free Software Foundation, Inc.
 * 51 Franklin Street, Fifth Floor
 * Boston, MA  02110-1301  USA
 */
package org.hibernate.resource.transaction;

/**
 * Models the coordination of all transaction related flows.
 *
 * @author Steve Ebersole
 */
public interface TransactionCoordinator {
	/**
	 * Indicates an explicit request to join a transaction.  This is mainly intended to handle the JPA requirement
	 * around {@link javax.persistence.EntityManager#joinTransaction()}, and generally speaking only has an impact in
	 * JTA environments
	 */
	public void explicitJoin();

	/**
	 * Determine if there is an active transaction that this coordinator is already joined to.
	 *
	 * @return {@code true} if there is an active transaction this coordinator is already joined to; {@code false}
	 * otherwise.
	 */
	public boolean isJoined();

	/**
	 * Used by owner of the JdbcSession as a means to indicate that implicit joining should be done if needed.
	 */
	public void pulse();

	/**
	 * Get the PhysicalTransactionDelegate for this TransactionCoordinator for use by the local transaction
	 *
	 * todo : rename this to something like CoordinatorTransactionDelegate?  or TransactionDelegate?
	 *
	 * @return The PhysicalTransactionDelegate
	 */
	public PhysicalTransactionDelegate getPhysicalTransactionDelegate();

	/**
	 * Get access to the local registry of Synchronization instances
	 *
	 * @return The local Synchronization registry
	 */
	public SynchronizationRegistry getLocalSynchronizations();
}
