Proposal
====

This document discusses the JdbcSession proposal, which is intended as a replacement/redesign of the current
JDBC Connection and transaction handling.  There is still a lot of things to be determined here; it is, after all, a
proposal ;)


## Granularity of the JDBC abstraction API

First and foremost is the API we want to expose from a JdbcSession.  I did 2 approaches to compare/contract styles.
The first approach (org.hibernate.resource2.jdbc package) largely follows what we have in Hibernate today; a very granular
low-level API.  The client of this API is expected to make a lot of decisions, make a lot of "little" calls based on
those decisions and do a lot of error handling.  In many ways what we have is a *very* thin wrapper over JDBC.  For
example, using JDBC directly we would have code like:

```java
final Connection conn = ...;
try {
    PreparedStatement ps = conn.prepareStatement( ... );
    try {
        setOptions( ps, ... );
        bindValues( ps, ... );
        ResultSet rs = ps.executeQuery();
        try {
            return extractResults( rs, ... );
        }
        catch( SQLException e )  {
            // from extraction
            ...
        }
        finally {
            close( rs );
        }
    }
    catch( SQLException e ) {
        // from setting/binding or from executing
        ...
    }
    finally {
        close( ps );
    }
}
catch( SQLException e ) {
    // from preparing the statement
    ...
}
finally {
    close( conn );
}
```

Using our "thin wrapper" we'd have something more like:

```java
final JdbcSession jdbcSession = ...;
try {
    // NOTE : SQLExceptions are handled in JdbcSession calls
    PreparedStatement ps = jdbcSession.prepareStatement( ... );
    try {
		ResultSet rs = jdbcSession.executeQuery( ps );
		try {
			return extractResults( rs, ... );
		}
        finally {
            jdbcSession.release( rs, ps );
        }
    }
    finally {
        jdbcSession.release( ps );
    }
}
finally {
    afterStatement( jdbcSession );
}
```

Still very similar feeling code.

A very different feel can be seen in second approach (org.hibernate.resource.jdbc package):

```java
return jdbcSession.accept(
    new PreparedStatementQueryOperation() {
        @Override
        public StatementPreparer getStatementPreparer() {
            return new StandardQueryStatementPreparer();
        }
        @Override
        public StatementExecutor<ResultSet> getStatementExecutor() {
            return new StandardQueryStatementExecutor();
        }
        @Override
        public ResultSetProcessor getResultSetProcessor() {
            return ...;
        }
        @Override
        public boolean holdOpenResources() {
            return false;
        }
    }
);
```

Here you can visualize the jdbcSession.accept() call as a boundary/scope, which coordinates processing between
different pieces (delegates) to achieve a whole:

```java
public Object accept(PreparedStatementQueryOperation operation) {
    try {
        Statement statement = operation.getStatementPreparer().prepare( this );
        try {
            ResultSet resultSet = operation.getStatementExecutor().execute( statement, this );
            try {
                return operation.getResultSetProcessor().extractResults( resultSet, this );
            }
            finally {
                if ( !operation.holdOpenResources() ) {
                    release( resultSet, statement );
                }
            }
        }
        finally {
            if ( !operation.holdOpenResources() ) {
                release( ps );
            }
        }
    }
    finally {
        afterStatement( !operation.holdOpenResources() );
    }
}
```

The "concern" is the sheer number of categorizations for ways we interact with JDBC all of which would need to be
modeled for this to work.  On the bright side we could limit this to just the categorizations we use; for
example executing a query via Statement and getting back a Result set is a valid categorization, but we never
do that.  But on the plus side, all variances in the general structure for handling the different categorizations
can be centralized; today they are duplicated across the code base.


## Transaction handling

A few changes are proposed here in terms of the conceptualization of transactions.

* First is the split in the notions of local (or logical) transactions versus physical transactions.  Local (or logical)
transactions are the Transaction instances used by Session/EntityManager clients to control transaction boundaries.  In
existing Hibernate code, _org.hibernate.Transaction_ actually plays both roles with the physical aspect being the
concrete implementation of the interface that gets used.  Essentially we now have delegation, rather than sub-classing,
for transaction driving.
* A second change is to further split JDBC-based versus JTA-based transaction handling.  Here we have a contract
called TransactionCoordinator whose overall role is roughly the same as the _org.hibernate.engine.transaction.spi.TransactionCoordinator_
contract in the current Hibernate code.  However, the way in which that contract is implemented here is much different.  In the existing code, the TransactionCoordinator impl actually handles both JDBC and JTA cases, meaning it is very complex with lots of if statements for the various cases.  The proposed change here is to instead have different implementations specific to JDBC/JTA cases.
* For JTA environments, I would love to always drive "transaction reaction" from a Synchronization registered with
the JTA transaction.  I am just not sure how feasible it is to say this will *always* be an option (e.g., some
JTA TransactionManagers won't let you register a Synchronization against a transaction that has been marked for
"rollback only").  Need to think through how real these problem cases are in our use cases.  This desire is really more
about simplification and maintainability.  The current Hibernate JTA-based Transaction code prefers to not do this, and
there are some advantages to driving the transaction locally when possible.  The problem is that catering for both
models at the same time leads to some very ugly and hard-to-maintain code.  If we need to do, we'll need to do it; would
just be nice to clean this up.

As I mentioned, TransactionCoordinator is responsible for the coordination of transaction related stuff.  There is a
synergy here between JdbcSession and TransactionCoordinator: the JdbcSession manages the TransactionCoordinator;
the TransactionCoordinator has access to the JdbcSession.  This leads to a nice flow of "events" between the two of
them.  Let's take a closer look at the specifics of that flow in regards to the JDBC and JTA env use-cases...

_BTW, this is unified between both "JDBC API granularity" approaches_
_TODO: Would it somehow be possible to support type simultaneously?  Maybe by passing some kind of argument when beginning a transaction?_

### JDBC case

In terms of the backend resources we'd have a JdbcSession impl with a TransactionCoordinatorJdbcImpl concrete
TransactionCoordinator implementation.  On the "local transaction" side we'd have org.hibernate.Transaction be given
a reference to the PhysicalTransactionInflow acquired from the TransactionCoordinator (TransactionCoordinatorJdbcImpl).
TransactionCoordinatorJdbcImpl.PhysicalTransactionInflow further gets access to JdbcSession's PhysicalJdbcTransaction
for delegation.  Sounds complex, but its not really :)  Let's take a look at the flow of various calls:

* Transaction.begin() would call
    * TransactionCoordinatorJdbcImpl.PhysicalTransactionInflow.begin(), which would call
        * PhysicalJdbcTransaction.begin()
        * TransactionCoordinatorJdbcImpl.afterBegin() : _NOTE that JDBC based transaction handing has nothing to do "after transaction begin"_

* Transaction.commit() would call
    * TransactionCoordinatorJdbcImpl.PhysicalTransactionInflow.commit(), which would call
        * TransactionCoordinatorJdbcImpl.beforeCompletion()
        * PhysicalJdbcTransaction.commit()
        * TransactionCoordinatorJdbcImpl.afterCompletion( true )

* Transaction.rollback() would call
    * TransactionCoordinatorJdbcImpl.PhysicalTransactionInflow.rollback(), which would call
        * _no beforeCompletion callbacks, consistent with JTA spec_
        * PhysicalJdbcTransaction.rollback()
        * TransactionCoordinatorJdbcImpl.afterCompletion( false )


### JTA case(s)

This is still a work-in-progress atm.  So the specifics of how the back-end pieces fit together might change, but in
general the conceptualization is much the same as the JDBC case:

In terms of the backend resources we'd have a JdbcSession impl with a TransactionCoordinatorJtaImpl concrete
TransactionCoordinator implementation.  On the "local transaction" side we'd have org.hibernate.Transaction be given
a reference to the PhysicalTransactionInflow acquired from the TransactionCoordinator (TransactionCoordinatorJtaImpl).
The TransactionCoordinatorJtaImpl is handed a JtaPlatform instance it can use to access the UserTransaction and/or
TransactionManager as needed; therefore the PhysicalTransactionInflow it produces has access to that as well.

Again this is more complex in its explanation than it is in reality :)  The flows:

_As mentioned above, a proposed conceptual change here is to always have the JTA based transaction handling register
a JTA Synchronization and to always use that Synchronization to drive the coordinator.  Registration of
the Synchronization would still depend on the (auto)join/begin rules.  For the flows below, let's assume we always
register the Synchronization_

_Currently there is no concept of a PhysicalJtaTransaction.  This is just a unified delegate for
UserTransaction/TransactionManager calls_

* Transaction.begin() would call
    * TransactionCoordinatorJtaImpl.PhysicalTransactionInflow.begin(), which would call
        * PhysicalJtaTransaction.begin()
        * TransactionCoordinatorJtaImpl.afterBegin()

* Transaction.commit() would call
    * TransactionCoordinatorJtaImpl.PhysicalTransactionInflow.commit(), which would call
        * PhysicalJtaTransaction.begin(), which would *lead to*:
            * RegisteredSynchronization.beforeCompletion(), which would call
        	    * SynchronizationCallbackCoordinator.beforeCompletion(), which would call
        	        * TransactionCoordinatorJtaImpl.beforeCompletion() - via SynchronizationCallbackTarget
            * RegisteredSynchronization.afterCompletion()
        	    * SynchronizationCallbackCoordinator.afterCompletion(), which would call
        	        * TransactionCoordinatorJtaImpl.afterCompletion() - via SynchronizationCallbackTarget

* Transaction.rollback() would call
    * TransactionCoordinatorJtaImpl.PhysicalTransactionInflow.rollback(), which would call
        * PhysicalJtaTransaction.rollback(), which would *lead to*:
            * _again, note no beforeCompletion callbacks, consistent with JTA spec_
            * RegisteredSynchronization.afterCompletion()
        	    * SynchronizationCallbackCoordinator.afterCompletion(), which would call
        	        * TransactionCoordinatorJtaImpl.afterCompletion() - via SynchronizationCallbackTarget




