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
package org.hibernate.resource2.jdbc;

import java.sql.Connection;

import org.hibernate.engine.jdbc.spi.StatementPreparer;
import org.hibernate.resource2.transaction.TransactionCoordinator;

/**
 * Models the context of a JDBC Connection and transaction.  The name comes from the database view where what is
 * represented by the JDBC Connection (and associated transaction) is called a "session".
 *
 * @author Steve Ebersole
 */
public interface JdbcSession extends StatementPreparer, StatementExecutor, ResourceRegistry {
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
	 * Closes the JdbcSession, making it inactive and forcing release of any held resources
	 *
	 * @return Legacy :(  Returns the JDBC Connection *if* the user passed in a Connection originally.
	 */
	public Connection close();

	/**
	 * Is this JdbcSession currently physically connected (meaning does it currently hold a JDBC Connection)?
	 *
	 * @return {@code true} if the JdbcSession currently hold a JDBC Connection; {@code false} if it does not.
	 */
	public boolean isPhysicallyConnected();

	/**
	 * Set the transaction time-out in seconds.  We will use this (if set) to appropriately apply statement time-outs
	 * to any statements performed from this JdbcSession
	 *
	 * @param seconds The number of seconds
	 */
	public void setTransactionTimeOut(int seconds);

	/**
	 * Accept some ad-hoc work to be done within bounds of this JdbcSession
	 *
	 * @param work The work to perform
	 * @param <T> The work result type
	 *
	 * @return The work result, see {@link Work#perform}
	 */
	public <T> T accept(Work<T> work);

	/**
	 * Accept an operation to be performed within bounds of this JdbcSession
	 *
	 * @param operation The operation to perform
	 * @param <T> The operation result type
	 *
	 * @return The operation result, see {@link Operation#perform}
	 */
	public <T> T accept(Operation<T> operation);

	/**
	 * Attempt to cancel the last query statement.
	 */
	public void cancelLastQuery();

	/**
	 * Is the JdbcSession in a state that would allow it to be serialized?
	 *
	 * @return {@code true} indicates the JdbcSession, as is, can be serialized.
	 */
	public boolean isReadyToSerialize();


	// todo : still to add ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
//	public LobCreator getLobCreator();
//	public BatchBuilder getBatchBuilder();

}
