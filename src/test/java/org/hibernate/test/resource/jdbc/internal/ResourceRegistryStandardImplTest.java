package org.hibernate.test.resource.jdbc.internal;

import java.sql.SQLException;
import java.sql.Statement;

import org.hibernate.resource.jdbc.ResourceRegistry;
import org.hibernate.resource.jdbc.internal.ResourceRegistryStandardImpl;

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
	public void hasRegisterShlouldReturnFalseIfNoResourcesAreRegister() {
		assertThat( registry.hasRegisteredResources(), is( false ) );
	}

	@Test
	public void hasRegisterShouldReturnTrueIfOneresourceIsRegistere() {
		Statement statement = mock( Statement.class );

		registry.register( statement, false );

		assertThat( registry.hasRegisteredResources(), is( true ) );

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
	public void shouldCloseAndUnregisterAStatementAndresulsets() throws SQLException {
		Statement statement = mock( Statement.class );

		registry.register( statement, false );

		registry.release( statement );


		assertThat( registry.hasRegisteredResources(), is( false ) );
		verify( statement ).close();
	}


}