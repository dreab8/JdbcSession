package org.hibernate.resource2.jdbc.spi;

import org.hibernate.resource2.transaction.TransactionCoordinator;

/**
 * @author Steve Ebersole
 */
public interface TransactionCoordinatorBuilder {
	public TransactionCoordinator buildTransactionCoordinator(JdbcSessionImplementor jdbcSession);
}
