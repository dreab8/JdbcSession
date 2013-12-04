package org.hibernate.test.resource.jdbc;

import java.sql.Connection;

import org.hibernate.resource.jdbc.JdbcSession;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Steve Ebersole
 */
public abstract class AbstractBasicUsageTest {
	protected abstract Connection getConnection();
	protected abstract JdbcSession getJdbcSession();

	@Test
	public void testSimpleTransactionNoWork() throws Exception {
		assertTrue( getConnection().getAutoCommit() );
		getJdbcSession().getTransactionCoordinator().getPhysicalTransactionInflow().begin();
		assertFalse( getConnection().getAutoCommit() );
		getJdbcSession().getTransactionCoordinator().getPhysicalTransactionInflow().commit();
		assertTrue( getConnection().getAutoCommit() );
	}
}
