package org.hibernate.test.jdbc.common;

import org.hibernate.ConnectionReleaseMode;
import org.hibernate.engine.jdbc.spi.SqlExceptionHelper;
import org.hibernate.engine.jdbc.spi.SqlStatementLogger;
import org.hibernate.resource2.jdbc.spi.JdbcSessionContext;
import org.hibernate.resource2.jdbc.spi.JdbcObserver;
import org.hibernate.resource2.jdbc.spi.StatementInspector;

/**
 * @author Steve Ebersole
 */
public class JdbcSessionContextStandardTestingImpl implements JdbcSessionContext {
	/**
	 * Singleton access
	 */
	public static final JdbcSessionContextStandardTestingImpl INSTANCE = new JdbcSessionContextStandardTestingImpl();

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
	public StatementInspector getStatementInspector() {
		return StatementInspectorNoOpImpl.INSTANCE;
	}

	@Override
	public SqlExceptionHelper getSqlExceptionHelper() {
		return new SqlExceptionHelper( SimpleSQLExceptionConverter.INSTANCE );
	}

	@Override
	public SqlStatementLogger getSqlStatementLogger() {
		return new SqlStatementLogger();
	}

	@Override
	public JdbcObserver getObserver() {
		return JdbcObserverNoOpImpl.INSTANCE;
	}
}
