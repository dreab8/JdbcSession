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
package org.hibernate.resource.store.jdbc;

import java.sql.Connection;

import org.hibernate.resource.store.DataStoreSessionFactory;
import org.hibernate.resource.store.DataStoreSessionOwner;
import org.hibernate.resource.store.jdbc.internal.JdbcSessionImpl;
import org.hibernate.resource.store.jdbc.internal.LogicalConnectionManagedImpl;
import org.hibernate.resource.store.jdbc.internal.LogicalConnectionProvidedImpl;
import org.hibernate.resource.store.jdbc.spi.JdbcSessionOwner;

/**
 * @author Steve Ebersole
 */
public class JdbcSessionFactory implements DataStoreSessionFactory {
	/**
	 * Singleton access
	 */
	public static final JdbcSessionFactory INSTANCE = new JdbcSessionFactory();

	@Override
	public JdbcSession create(DataStoreSessionOwner owner) {
		final JdbcSessionOwner jdbcSessionOwner = (JdbcSessionOwner) owner;
		final LogicalConnectionManagedImpl logicalConnection = new LogicalConnectionManagedImpl(
				jdbcSessionOwner.getJdbcConnectionAccess(),
				jdbcSessionOwner.getJdbcSessionContext()
		);
		return new JdbcSessionImpl(
				jdbcSessionOwner.getJdbcSessionContext(),
				logicalConnection,
				owner.getTransactionCoordinatorBuilder()
		);
	}

	@Override
	public JdbcSession create(DataStoreSessionOwner owner, Connection jdbcConnection) {
		final JdbcSessionOwner jdbcSessionOwner = (JdbcSessionOwner) owner;
		final LogicalConnectionProvidedImpl logicalConnection = new LogicalConnectionProvidedImpl( jdbcConnection );
		return new JdbcSessionImpl(
				jdbcSessionOwner.getJdbcSessionContext(),
				logicalConnection,
				owner.getTransactionCoordinatorBuilder()
		);
	}
}
