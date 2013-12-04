package org.hibernate.test.resource.jdbc.common;

import org.hibernate.resource.jdbc.spi.StatementInspector;

/**
 * @author Steve Ebersole
 */
public class StatementInspectorNoOpImpl implements StatementInspector {
	/**
	 * Singleton access
	 */
	public static final StatementInspectorNoOpImpl INSTANCE = new StatementInspectorNoOpImpl();

	@Override
	public String inspect(String sql) {
		return sql;
	}
}
