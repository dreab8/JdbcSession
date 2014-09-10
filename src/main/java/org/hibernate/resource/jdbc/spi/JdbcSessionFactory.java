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
package org.hibernate.resource.jdbc.spi;

import java.sql.Connection;

import org.hibernate.resource.jdbc.JdbcSession;
import org.hibernate.resource.jdbc.ResourceRegistry;
import org.hibernate.resource.jdbc.internal.JdbcSessionImpl;
import org.hibernate.resource.jdbc.internal.LogicalConnectionManagedImpl;
import org.hibernate.resource.jdbc.internal.LogicalConnectionProvidedImpl;

/**
 * @author Steve Ebersole
 */
public class JdbcSessionFactory {
	/**
	 * Singleton access
	 */
	public static final JdbcSessionFactory INSTANCE = new JdbcSessionFactory();

    private JdbcSessionFactory(){
    }

	public JdbcSession create(JdbcSessionOwner owner) {
		final LogicalConnectionManagedImpl logicalConnection = new LogicalConnectionManagedImpl(
				owner.getJdbcConnectionAccess(),
				owner.getJdbcSessionContext()
		);
		return new JdbcSessionImpl(
				owner.getJdbcSessionContext(),
				logicalConnection,
				owner.getTransactionCoordinatorBuilder(),
				owner.getBatchFactory()
		);
	}

	public JdbcSession create(JdbcSessionOwner owner, Connection jdbcConnection) {
		final LogicalConnectionProvidedImpl logicalConnection = new LogicalConnectionProvidedImpl( jdbcConnection );
		return new JdbcSessionImpl(
				owner.getJdbcSessionContext(),
				logicalConnection,
				owner.getTransactionCoordinatorBuilder(),
				owner.getBatchFactory()
		);
	}

	public JdbcSession create(JdbcSessionOwner owner, ResourceRegistry resourceRegistry) {
		final LogicalConnectionManagedImpl logicalConnection = new LogicalConnectionManagedImpl(
				owner.getJdbcConnectionAccess(),
				owner.getJdbcSessionContext(),
				resourceRegistry
		);
		return new JdbcSessionImpl(
				owner.getJdbcSessionContext(),
				logicalConnection,
				owner.getTransactionCoordinatorBuilder(),
				owner.getBatchFactory()
		);
	}
}
