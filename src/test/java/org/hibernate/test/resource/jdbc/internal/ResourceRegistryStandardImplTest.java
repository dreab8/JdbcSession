package org.hibernate.test.resource.jdbc.internal;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.hibernate.resource.jdbc.ResourceRegistry;
import org.hibernate.resource.jdbc.internal.ResourceRegistryStandardImpl;
import org.hibernate.resource.jdbc.spi.Batch;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ResourceRegistryStandardImplTest {
	private static ResourceRegistry registry;

	@Before
	public void serUp() {
		registry = new ResourceRegistryStandardImpl();
	}

	@After
	public void tearDown() {
		registry = null;
	}

	@Test
	public void shouldHasRegisterReturnFalseIfNoResourcesAreRegistered() {
		assertThat( registry.hasRegisteredResources(), is( false ) );
	}

	@Test
	public void shpouldHasRegisterReturnTrueIfOneResourceIsRegistered() {
		Statement statement = mock( Statement.class );

		registry.register( statement, false );

		assertThat( registry.hasRegisteredResources(), is( true ) );
	}

	@Test
	public void shouldHasRegisterReturnTrueIfABatchResourceIsRegistered() {
		Batch batch = mock( Batch.class );

		registry.register( batch );

		assertThat( registry.hasRegisteredResources(), is( true ) );
	}

	@Test
	public void releaseBatchShouldCallBatchReleaseMethod(){
		Batch batch = mock( Batch.class );

		registry.register( batch );

		registry.releaseCurrentBatch();

		verify( batch ).release();

		assertThat( registry.hasRegisteredResources(), is( false ) );
	}


	@Test
	public void shouldCloseAndUnregisterAStatement() throws SQLException {
		Statement statement = mock( Statement.class );

		registry.register( statement, false );

		registry.release( statement );

		assertThat( registry.hasRegisteredResources(), is( false ) );
		verify( statement ).close();
	}

	@Test
	public void shouldCloseAndUnregisterAStatementAndItsResultSets() throws SQLException {
		Statement statement = mock( Statement.class );
		ResultSet resultSet1 = mock( ResultSet.class );
		ResultSet resultSet2 = mock( ResultSet.class );

		registry.register( resultSet1, statement );
		registry.register( resultSet2, statement );

		registry.release( resultSet1, statement );

		assertThat( registry.hasRegisteredResources(), is( false ) );
		verify( statement ).close();
		verify( resultSet1 ).close();
		verify( resultSet2 ).close();
	}

	@Test
	public void shouldCloseAndUnregisterAResultSetNotAssociatedWithAStatement() throws SQLException {
		ResultSet resultSet = mock( ResultSet.class );

		registry.register( resultSet, null );

		registry.release( resultSet, null );

		assertThat( registry.hasRegisteredResources(), is( false ) );
		verify( resultSet ).close();
	}
}