package org.hibernate.test.resource.jdbc.common;

import org.hibernate.ConnectionReleaseMode;
import org.hibernate.engine.jdbc.spi.SqlExceptionHelper;
import org.hibernate.engine.jdbc.spi.SqlStatementLogger;
import org.hibernate.resource.jdbc.spi.JdbcObserver;
import org.hibernate.resource.jdbc.spi.JdbcSessionContext;
import org.hibernate.resource.jdbc.spi.StatementInspector;

/**
 * @author Steve Ebersole
 */
public class JdbcSessionContextStandardTestingImpl implements JdbcSessionContext {
	/**
	 * Singleton access
	 */
	public static final JdbcSessionContextStandardTestingImpl INSTANCE = new JdbcSessionContextStandardTestingImpl();

	private final SqlStatementLogger sqlStatementLogger = new SqlStatementLogger();
	private final SqlExceptionHelper sqlExceptionHelper = new SqlExceptionHelper( SimpleSQLExceptionConverter.INSTANCE );

	@Override
	public boolean isScrollableResultSetsEnabled() {
		return false;
	}

	@Override
	public boolean isGetGeneratedKeysEnabled() {
		return false;
	}

	@Override
	public int getFetchSize() {
		return -1;
	}

	@Override
	public ConnectionReleaseMode getConnectionReleaseMode() {
		return ConnectionReleaseMode.ON_CLOSE;
	}

	@Override
	public ConnectionAcquisitionMode getConnectionAcquisitionMode() {
		return ConnectionAcquisitionMode.DEFAULT;
	}

	@Override
	public StatementInspector getStatementInspector() {
		return StatementInspectorNoOpImpl.INSTANCE;
	}

	@Override
	public SqlExceptionHelper getSqlExceptionHelper() {
		return sqlExceptionHelper;
	}

	@Override
	public SqlStatementLogger getSqlStatementLogger() {
		return sqlStatementLogger;
	}

	@Override
	public JdbcObserver getObserver() {
		return JdbcObserverNoOpImpl.INSTANCE;
	}
}
