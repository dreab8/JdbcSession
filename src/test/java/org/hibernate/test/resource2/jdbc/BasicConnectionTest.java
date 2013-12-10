package org.hibernate.test.resource2.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.hibernate.JDBCException;
import org.hibernate.dialect.H2Dialect;

import org.hibernate.test.resource2.jdbc.common.JdbcSessionContextStandardTestingImpl;

import org.hibernate.resource2.jdbc.JdbcSession;
import org.hibernate.resource2.jdbc.internal.LogicalConnectionProvidedImpl;
import org.hibernate.resource2.jdbc.spi.JdbcSessionImplementor;
import org.hibernate.resource2.jdbc.spi.TransactionCoordinatorBuilder;
import org.hibernate.resource2.transaction.TransactionCoordinator;
import org.hibernate.resource2.transaction.internal.TransactionCoordinatorJdbcImpl;
import org.hibernate.resource2.jdbc.internal.JdbcSessionImpl;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Steve Ebersole
 */
public class BasicConnectionTest {
	private Connection jdbcConnection;
	private JdbcSession jdbcSession;


	@Before
	public void setUp() throws Exception {
		final Driver driver = (Driver) Class.forName( "org.h2.Driver" ).newInstance();
		final String url = "jdbc:h2:mem:db1";
		Properties connectionProperties = new Properties();
		connectionProperties.setProperty( "username", "sa" );
		connectionProperties.setProperty( "password", "" );

		jdbcConnection = driver.connect( url, connectionProperties );

		jdbcSession = new JdbcSessionImpl(
				JdbcSessionContextStandardTestingImpl.INSTANCE,
				new LogicalConnectionProvidedImpl( jdbcConnection ),
				new TransactionCoordinatorBuilder() {
					@Override
					public TransactionCoordinator buildTransactionCoordinator(JdbcSessionImplementor jdbcSession) {
						return new TransactionCoordinatorJdbcImpl( jdbcSession );
					}
				}
		);
	}

	@After
	public void tearDown() throws Exception {
		jdbcSession.close();
		jdbcConnection.close();
	}

	protected Connection getConnection() {
		return jdbcConnection;
	}

	protected JdbcSession getJdbcSession() {
		return jdbcSession;
	}

	@Test
	public void testSimpleTransactionNoWork() throws Exception {
		assertTrue( getConnection().getAutoCommit() );
		getJdbcSession().getTransactionCoordinator().getPhysicalTransactionInflow().begin();
		assertFalse( getConnection().getAutoCommit() );
		getJdbcSession().getTransactionCoordinator().getPhysicalTransactionInflow().commit();
		assertTrue( getConnection().getAutoCommit() );
	}

	@Test
	public void testExceptionHandling() {
		try {
			getJdbcSession().prepareStatement( "select count(*) from NON_EXISTENT" );
			fail( "Expected an exception" );
		}
		catch (JDBCException expected) {
		}
		catch (Exception e) {
			fail( "Expecting JDBCException, but got " + e.getClass().getName() );
		}
	}

	@Test
	public void testBasicJdbcUsage() throws JDBCException {
		try {
			Statement statement = getJdbcSession().createStatement();
			String dropSql = new H2Dialect().getDropTableString( "SANDBOX_JDBC_TST" );
			try {
				getJdbcSession().executeDdl( statement, dropSql );
			}
			catch ( Exception e ) {
				// ignore if the DB doesn't support "if exists" and the table doesn't exist
			}
			String createSql = "create table SANDBOX_JDBC_TST ( ID integer, NAME varchar(100) )";
			getJdbcSession().executeDdl( statement, createSql );

			assertTrue( getJdbcSession().hasRegisteredResources() );
			assertTrue( getJdbcSession().isPhysicallyConnected() );

			getJdbcSession().release( statement );
			assertFalse( getJdbcSession().hasRegisteredResources() );
			assertTrue( getJdbcSession().isPhysicallyConnected() );

			PreparedStatement ps = getJdbcSession().prepareStatement(
					"insert into SANDBOX_JDBC_TST( ID, NAME ) values ( ?, ? )"
			);
			ps.setLong( 1, 1 );
			ps.setString( 2, "name" );
			int count = getJdbcSession().executeUpdate( ps );
			assertEquals( 1, count );

			assertTrue( getJdbcSession().hasRegisteredResources() );

			ps = getJdbcSession().prepareStatement( "select * from SANDBOX_JDBC_TST" );
			ResultSet rs = getJdbcSession().executeQuery( ps );
			assertNotNull( rs );
			assertTrue( rs.next() );
			assertFalse( rs.next() );

			assertTrue( getJdbcSession().hasRegisteredResources() );
		}
		catch ( SQLException e ) {
			fail( "incorrect exception type : sqlexception" );
		}

		getJdbcSession().close();
		assertFalse( getJdbcSession().hasRegisteredResources() );
	}
}
