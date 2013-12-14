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
import java.sql.Driver;
import java.util.Properties;

import org.hibernate.resource.store.jdbc.JdbcSession;
import org.hibernate.resource.store.jdbc.internal.JdbcSessionImpl;
import org.hibernate.resource.store.jdbc.internal.LogicalConnectionProvidedImpl;
import org.hibernate.resource.store.jdbc.spi.JdbcSessionImplementor;
import org.hibernate.resource.store.jdbc.spi.TransactionCoordinatorBuilder;
import org.hibernate.resource.transaction.TransactionCoordinator;
import org.hibernate.resource.transaction.TransactionCoordinatorBuilderFactory;
import org.hibernate.resource.transaction.internal.TransactionCoordinatorResourceLocalImpl;

import org.hibernate.test.resource.jdbc.common.JdbcSessionContextStandardTestingImpl;
import org.junit.After;
import org.junit.Before;

/**
 * @author Steve Ebersole
 */
public class BasicJdbcUsageTestWithProvidedConnections extends AbstractBasicJdbcUsageTest {
	private Connection jdbcConnection;
	private JdbcSessionImpl jdbcSession;

	@Override
	protected Connection getConnection() {
		return jdbcConnection;
	}

	@Override
	protected JdbcSession getJdbcSession() {
		return jdbcSession;
	}

	@Before
	public void setUp() throws Exception {
		final Driver driver = (Driver) Class.forName( "org.h2.Driver" ).newInstance();
		final String url = "jdbc:h2:mem:db1";
		Properties connectionProperties = new Properties();
		connectionProperties.setProperty( "username", "sa" );
		connectionProperties.setProperty( "password", "" );

		jdbcConnection = driver.connect( url, connectionProperties );

		jdbcSession = new JdbcSessionImpl(
				JdbcSessionContextStandardTestingImpl.INSTANCE,
				new LogicalConnectionProvidedImpl( jdbcConnection ),
				TransactionCoordinatorBuilderFactory.INSTANCE.forResourceLocal()
		);
	}

	@After
	public void tearDown() throws Exception {
		jdbcSession.close();
		jdbcConnection.close();
	}
}
