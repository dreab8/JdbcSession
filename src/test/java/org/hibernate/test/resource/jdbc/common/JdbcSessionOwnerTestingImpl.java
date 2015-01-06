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
package org.hibernate.test.resource.jdbc.common;

import java.sql.Connection;
import java.sql.SQLException;

import org.hibernate.resource.jdbc.internal.BatchBuilderImpl;
import org.hibernate.resource.jdbc.spi.BatchBuilder;
import org.hibernate.resource.jdbc.spi.JdbcConnectionAccess;
import org.hibernate.resource.jdbc.spi.JdbcSessionContext;
import org.hibernate.resource.jdbc.spi.JdbcSessionOwner;
import org.hibernate.resource.transaction.TransactionCoordinatorBuilder;
import org.hibernate.resource.transaction.TransactionCoordinatorBuilderFactory;

import org.hibernate.test.resource.common.DatabaseConnectionInfo;

/**
 * @author Steve Ebersole
 */
public class JdbcSessionOwnerTestingImpl implements JdbcSessionOwner {

	private BatchBuilder batchBuilder = new BatchBuilderImpl( 0 );
	private JdbcSessionContext jdbcSessionContext = JdbcSessionContextStandardTestingImpl.INSTANCE;
	private JdbcConnectionAccess jdbcConnectionAccess = new JdbcConnectionAccess() {
		@Override
		public Connection obtainConnection() throws SQLException {
			return DatabaseConnectionInfo.INSTANCE.makeConnection();
		}

		@Override
		public void releaseConnection(Connection connection) throws SQLException {
			connection.close();
		}
	};
	private TransactionCoordinatorBuilder transactionCoordinatorBuilder = TransactionCoordinatorBuilderFactory.INSTANCE.forResourceLocal();

	public JdbcSessionOwnerTestingImpl() {
	}

	@Override
	public JdbcSessionContext getJdbcSessionContext() {
		return jdbcSessionContext;
	}

	public void setJdbcSessionContext(JdbcSessionContext jdbcSessionContext) {
		this.jdbcSessionContext = jdbcSessionContext;
	}

	@Override
	public JdbcConnectionAccess getJdbcConnectionAccess() {
		return jdbcConnectionAccess;
	}

	public void setJdbcConnectionAccess(JdbcConnectionAccess jdbcConnectionAccess) {
		this.jdbcConnectionAccess = jdbcConnectionAccess;
	}

	@Override
	public TransactionCoordinatorBuilder getTransactionCoordinatorBuilder() {
		return transactionCoordinatorBuilder;
	}

	@Override
	public BatchBuilder getBatchBuilder() {
		return batchBuilder;
	}

	public void setTransactionCoordinatorBuilder(TransactionCoordinatorBuilder transactionCoordinatorBuilder) {
		this.transactionCoordinatorBuilder = transactionCoordinatorBuilder;
	}

	@Override
	public void beforeTransactionCompletion() {
	}

	@Override
	public void afterTransactionCompletion(boolean successful) {
	}

	public void setBatchBuilder(BatchBuilder batchBuilder){
		this.batchBuilder = batchBuilder;
	}
}
