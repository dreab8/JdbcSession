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
 * Models the context of a JDBC Connection and transaction.  The name comes from the database view where what is
 * represented by the JDBC Connection (and associated transaction) is called a "session".
 *
 * @author Steve Ebersole
 */
public interface JdbcSession {
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
	 * @return The
	 *
	 * @throws org.hibernate.ResourceClosedException if the JdbcSession is closed
	 */
	public TransactionCoordinator getTransactionCoordinator();

	/**
	 * Is this JdbcSession still open/active.  In other words, has {@link #close} not been called yet?
	 *
	 * @return {@code true} if the JdbcSession is still open ({@link #close} has not been called yet); {@code false}
	 * if the JdbcSession is not open (({@link #close} has been called).
	 */
	public boolean isOpen();

	/**
	 * Closes the JdbcSession, making it inactive and forcing release of any held resources.
	 *
	 * @throws org.hibernate.ResourceClosedException if the JdbcSession is closed
	 */
	public void close();

	/**
	 * Is the JdbcSession in a state that would allow it to be serialized?
	 *
	 * @return {@code true} indicates the JdbcSession, as is, can be serialized.
	 */
	public boolean isReadyToSerialize();

	/**
	 * Accept an operation to be performed within bounds of this JdbcSession
	 *
	 * @param operation The operation to perform
	 * @param <T> The operation result type
	 *
	 * @return The operation result, see {@link org.hibernate.resource2.jdbc.Operation#perform}
	 */
	public <T> T accept(Operation<T> operation);

}
