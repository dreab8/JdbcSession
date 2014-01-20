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
package org.hibernate.resource.transaction.backend.local.internal;

import org.hibernate.HibernateException;
import org.hibernate.resource.transaction.TransactionCoordinator;
import org.hibernate.resource.transaction.backend.local.spi.ResourceLocalTransactionAccess;
import org.hibernate.resource.transaction.spi.TransactionCoordinatorOwner;
import org.hibernate.resource.transaction.TransactionCoordinatorResourceLocalBuilder;

/**
 * Concrete builder for resource-local TransactionCoordinator instances.
 *
 * @author Steve Ebersole
 */
public class ResourceLocalTransactionCoordinatorBuilderImpl implements TransactionCoordinatorResourceLocalBuilder {
	private ResourceLocalTransactionAccess providedResourceLocalTransactionAccess;

	@Override
	public void setResourceLocalTransactionAccess(ResourceLocalTransactionAccess resourceLocalTransactionAccess) {
		this.providedResourceLocalTransactionAccess = resourceLocalTransactionAccess;
	}

	@Override
	public TransactionCoordinator buildTransactionCoordinator(TransactionCoordinatorOwner owner) {
		if ( providedResourceLocalTransactionAccess != null ) {
			return new ResourceLocalTransactionCoordinatorImpl( owner, providedResourceLocalTransactionAccess );
		}
		else {
			if ( owner instanceof ResourceLocalTransactionAccess ) {
				return new ResourceLocalTransactionCoordinatorImpl( owner, (ResourceLocalTransactionAccess) owner );
			}
		}

		throw new HibernateException(
				"Could not determine ResourceLocalTransactionAccess to use in building TransactionCoordinator"
		);
	}
}
