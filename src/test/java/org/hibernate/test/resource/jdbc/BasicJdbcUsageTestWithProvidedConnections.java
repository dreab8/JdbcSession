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

import org.hibernate.resource.jdbc.JdbcSession;
import org.hibernate.resource.jdbc.internal.JdbcSessionImpl;
import org.hibernate.resource.jdbc.internal.LogicalConnectionProvidedImpl;
import org.hibernate.resource.transaction.TransactionCoordinatorBuilderFactory;

import org.hibernate.test.resource.common.DatabaseConnectionInfo;
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
		jdbcConnection = DatabaseConnectionInfo.INSTANCE.makeConnection();

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
