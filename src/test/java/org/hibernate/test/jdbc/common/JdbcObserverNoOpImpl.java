package org.hibernate.test.jdbc.common;

import org.hibernate.resource2.jdbc.spi.JdbcObserver;

/**
 * @author Steve Ebersole
 */
public class JdbcObserverNoOpImpl implements JdbcObserver {
	/**
	 * Singleton access
	 */
	public static final JdbcObserverNoOpImpl INSTANCE = new JdbcObserverNoOpImpl();

	@Override
	public void jdbcConnectionAcquisitionStart() {
	}

	@Override
	public void jdbcConnectionAcquisitionEnd() {
	}

	@Override
	public void jdbcConnectionReleaseStart() {
	}

	@Override
	public void jdbcConnectionReleaseEnd() {
	}

	@Override
	public void jdbcPrepareStatementStart() {
	}

	@Override
	public void jdbcPrepareStatementEnd() {
	}

	@Override
	public void jdbcExecuteStatementStart() {
	}

	@Override
	public void jdbcExecuteStatementEnd() {
	}

	@Override
	public void jdbcExecuteBatchStart() {
	}

	@Override
	public void jdbcExecuteBatchEnd() {
	}
}
