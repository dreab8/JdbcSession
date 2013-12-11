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
package org.hibernate.test.resource.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import javax.transaction.Status;
import javax.transaction.TransactionManager;

import org.hibernate.cfg.AvailableSettings;
import org.hibernate.resource.jdbc.JdbcSession;
import org.hibernate.resource.jdbc.internal.JdbcSessionImpl;
import org.hibernate.resource.jdbc.internal.LogicalConnectionManagedImpl;
import org.hibernate.resource.jdbc.spi.JdbcConnectionAccess;
import org.hibernate.resource.jdbc.spi.JdbcSessionImplementor;
import org.hibernate.resource.jdbc.spi.TransactionCoordinatorBuilder;
import org.hibernate.resource.transaction.TransactionCoordinator;
import org.hibernate.resource.transaction.internal.TransactionCoordinatorJtaImpl;

import org.hibernate.test.resource.jdbc.common.ConnectionProviderJtaAwareImpl;
import org.hibernate.test.resource.jdbc.common.JdbcSessionContextStandardTestingImpl;
import org.hibernate.test.resource.jdbc.common.JtaPlatformStandardTestingImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Steve Ebersole
 */
public class BasicJtaUsageTest {
	private ConnectionProviderJtaAwareImpl connectionProvider;
	private JdbcSession jdbcSession;

	@Before
	public void setUp() throws Exception {
		HashMap<String,String> props = new HashMap<String,String>();
		props.put( AvailableSettings.DRIVER, "org.h2.Driver" );
		props.put( AvailableSettings.URL, "jdbc:h2:mem:db1" );
		props.put( AvailableSettings.USER, "sa" );
		props.put( AvailableSettings.PASS, "" );

		connectionProvider = new ConnectionProviderJtaAwareImpl();
		connectionProvider.configure( props );

		jdbcSession = new JdbcSessionImpl(
				JdbcSessionContextStandardTestingImpl.INSTANCE,
				new LogicalConnectionManagedImpl(
						new JdbcConnectionAccess() {
							@Override
							public Connection obtainConnection() throws SQLException {
								return connectionProvider.getConnection();
							}

							@Override
							public void releaseConnection(Connection connection) throws SQLException {
								connectionProvider.closeConnection( connection );
							}
						},
						JdbcSessionContextStandardTestingImpl.INSTANCE
				),
				new TransactionCoordinatorBuilder() {
					@Override
					public TransactionCoordinator buildTransactionCoordinator(JdbcSessionImplementor jdbcSession) {
						return new TransactionCoordinatorJtaImpl( jdbcSession, JtaPlatformStandardTestingImpl.INSTANCE, true, false );
					}
				}
		);
	}

	@After
	public void tearDown() throws Exception {
		jdbcSession.close();
		connectionProvider.stop();
	}


	@Test
	public void bmtUsageTest() throws Exception {
		final TransactionManager tm = JtaPlatformStandardTestingImpl.INSTANCE.transactionManager();
		assertEquals( Status.STATUS_NO_TRANSACTION, tm.getStatus() );

		jdbcSession.getTransactionCoordinator().getPhysicalTransactionDelegate().begin();
		assertEquals( Status.STATUS_ACTIVE, tm.getStatus() );

		jdbcSession.getTransactionCoordinator().getPhysicalTransactionDelegate().commit();
		assertEquals( Status.STATUS_NO_TRANSACTION, tm.getStatus() );
	}


}
