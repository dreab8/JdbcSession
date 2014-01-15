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
package org.hibernate.resource.jdbc;

import org.hibernate.resource.transaction.TransactionCoordinator;

/**
 * Models the context of a JDBC Connection and transaction.
 *
 * @author Steve Ebersole
 */
public interface JdbcSession {
	/**
	 * Is this JdbcSession still open/active.  In other words, has {@link #close} not been called yet?
	 *
	 * @return {@code true} if the JdbcSession is still open; {@code false} if it is closed.
	 */
	public boolean isOpen();

	/**
	 * Closes the JdbcSession, making it inactive and forcing release of any held resources.
	 *
	 * @throws org.hibernate.ResourceClosedException if the JdbcSession is already closed; todo make this no-op?
	 */
	public void close();

	/**
	 * Get the logical connection to the database represented by this JdbcSession
	 *
	 * @return The logical JDBC connection.
	 *
	 * @throws org.hibernate.ResourceClosedException if the JdbcSession is closed
	 */
	public LogicalConnection getLogicalConnection();

	/**
	 * Provides access to the TransactionCoordinator for this JdbcSession
	 *
	 * @return The TransactionCoordinator for this JdbcSession
	 *
	 * @throws org.hibernate.ResourceClosedException if the JdbcSession is closed
	 */
	public TransactionCoordinator getTransactionCoordinator();

	/**
	 * Accept an operation to be performed within bounds of this JdbcSession
	 *
	 * @param operation The operation to perform
	 * @param <R> The operation result type
	 *
	 * @return The operation result, see {@link Operation#perform}
	 */
	public <R> R accept(Operation<R> operation);

	/**
	 * Accept the specification of an operation to be performed within bounds of this JdbcSession
	 *
	 * @param operation The specification for the operation to perform
	 * @param <R> The result type
	 *
	 * @return The operation result
	 */
	public <R> R accept(OperationSpec<R> operation);
}
