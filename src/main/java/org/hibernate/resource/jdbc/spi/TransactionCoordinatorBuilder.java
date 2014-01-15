package org.hibernate.resource.jdbc.spi;

import org.hibernate.resource.transaction.TransactionCoordinator;

/**
 * @author Steve Ebersole
 */
public interface TransactionCoordinatorBuilder {
	public TransactionCoordinator buildTransactionCoordinator(JdbcSessionImplementor jdbcSession);
}
