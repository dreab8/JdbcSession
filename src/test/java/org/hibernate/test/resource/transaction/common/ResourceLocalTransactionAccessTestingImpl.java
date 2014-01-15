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
package org.hibernate.test.resource.transaction.common;

import java.sql.Connection;
import java.sql.SQLException;

import org.hibernate.TransactionException;
import org.hibernate.resource.transaction.backend.local.spi.ResourceLocalTransaction;
import org.hibernate.resource.transaction.backend.local.spi.ResourceLocalTransactionAccess;

import org.hibernate.test.resource.common.DatabaseConnectionInfo;

/**
 * @author Steve Ebersole
 */
public class ResourceLocalTransactionAccessTestingImpl
		extends TransactionCoordinatorOwnerTestingImpl
		implements ResourceLocalTransactionAccess, ResourceLocalTransaction {

	private final Connection jdbcConnection;

	public ResourceLocalTransactionAccessTestingImpl() throws Exception {
		jdbcConnection = DatabaseConnectionInfo.INSTANCE.makeConnection();
	}

	public Connection getJdbcConnection() {
		return jdbcConnection;
	}

	@Override
	public ResourceLocalTransaction getResourceLocalTransaction() {
		return this;
	}

	@Override
	public void begin() {
		try {
			jdbcConnection.setAutoCommit( false );
		}
		catch( SQLException e ) {
			throw new TransactionException( "JDBC begin transaction failed: ", e );
		}
	}

	@Override
	public void commit() {
		try {
			jdbcConnection.commit();
		}
		catch( SQLException e ) {
			throw new TransactionException( "JDBC begin transaction failed: ", e );
		}
	}

	@Override
	public void rollback() {
		try {
			jdbcConnection.rollback();
		}
		catch( SQLException e ) {
			throw new TransactionException( "JDBC begin transaction failed: ", e );
		}
	}
}
