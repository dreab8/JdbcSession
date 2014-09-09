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

import java.sql.SQLException;

import org.hibernate.resource.jdbc.JdbcSession;
import org.hibernate.resource.jdbc.spi.JdbcSessionFactory;
import org.hibernate.resource.jdbc.Operation;

import org.hibernate.test.resource.jdbc.common.JdbcSessionOwnerTestingImpl;

import org.junit.Test;

/**
 * @author Steve Ebersole
 */
public class BasicJdbcOperationTest {
	@Test
	public void basicUsageTest() {
		final JdbcSessionOwnerTestingImpl jdbcSessionOwner = new JdbcSessionOwnerTestingImpl();
		final JdbcSession jdbcSession = JdbcSessionFactory.INSTANCE.create( jdbcSessionOwner );

		// todo : create a JdbcOperationContext that exposes the registry, etc to the JdbcOperation impls

		// todo : integrate concept of IsolationDelegate : where?

		try {
			jdbcSession.accept(
					new Operation<Void>() {
						@Override
						public Void perform(JdbcSession jdbcSession) throws SQLException {
							return null;
						}
					}
			);
		}
		finally {
			jdbcSession.close();
		}
	}

}
