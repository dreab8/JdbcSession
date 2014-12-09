/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * Copyright (c) {DATE}, Red Hat Inc. or third-party contributors as
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
package org.hibernate.test.resource.jdbc.operationspec;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.hibernate.resource.jdbc.JdbcSession;
import org.hibernate.resource.jdbc.internal.BatchFactoryImpl;
import org.hibernate.resource.jdbc.internal.ResourceRegistryStandardImpl;
import org.hibernate.resource.jdbc.spi.BatchFactory;
import org.hibernate.resource.jdbc.spi.JdbcSessionFactory;

import org.junit.After;
import org.junit.Before;

import org.hibernate.test.resource.common.DatabaseConnectionInfo;
import org.hibernate.test.resource.jdbc.common.JdbcSessionOwnerTestingImpl;

/**
 * @author Andrea Boriero
 */
public abstract class AbstractOperationSpecIntegrationTest {

	private JdbcSession jdbcSession;
	private Connection localConnection;

	@Before
	public void setUp() throws SQLException {
		localConnection = DatabaseConnectionInfo.INSTANCE.makeConnection();
		localConnection.setAutoCommit( false );
		createTables();
		jdbcSession = createJdbSession();
	}

	@After
	public void tearDown() throws SQLException {
		dropTables();
		jdbcSession.close();
	}

	private JdbcSession createJdbSession() {
		JdbcSessionOwnerTestingImpl JDBC_SESSION_OWNER = new JdbcSessionOwnerTestingImpl();
		JDBC_SESSION_OWNER.setBatchFactory( getBatchFactory() );
		return JdbcSessionFactory.INSTANCE.create( JDBC_SESSION_OWNER, new ResourceRegistryStandardImpl() );
	}

	protected BatchFactory getBatchFactory() {
		return new BatchFactoryImpl( 1 );
	}

	public JdbcSession getJdbcSession() {
		return jdbcSession;
	}

	public Connection getLocalConnection() {
		return localConnection;
	}

	public void execute(String sql) throws SQLException {
		PreparedStatement statement = getLocalConnection().prepareStatement( sql );
		statement.execute();
	}

	protected void rollback() throws SQLException {
		localConnection.rollback();
	}

	protected void commit() throws SQLException {
		localConnection.commit();
	}

	protected abstract void createTables() throws SQLException;

	protected abstract void dropTables() throws SQLException;
}
