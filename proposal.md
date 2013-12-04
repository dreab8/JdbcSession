Proposal
====

This document discusses the JdbcSession proposal, which is intended as a replacement/redesign of the current
JDBC Connection and transaction handling.  There is still a lot of things to be determined here; it is, after all, a
proposal ;)


## Granularity of the JDBC abstraction API

First and foremost is the API we want to expose from a JdbcSession.  I did 2 approaches to compare/contract styles.
The first approach (org.hibernate.jdbc package) largely follows what we have in Hibernate today; a very granular
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

The big change I'd am introducing here is the split in the notions of local (or logical) transactions versus
physical transactions.  Local (or logical) transactions are the Transaction instances used by
Session/EntityManager clients to control transaction boundaries.  In existing Hibernate code,
_org.hibernate.Transaction_ actually plays both roles with the physical aspect being the concrete implementation
of the interface that gets used.

The other big change I want to look at is to always drive transaction reaction in JTA environments based on
a Synchronization.  The current Hibernate JTA-based Transaction code prefers to not do this, and there are some
advantages to driving the transaction locally when possible.  The problem is that catering for both models at
the same time leads to some very ugly and hard-to-maintain code.  So this is much more about simplification
and maintainability.

Essentially there are 2 flows: JDBC-based and JTA-based transactions.  Would it somehow be possible to support
type simultaneously?  Maybe by passing some kind of argument when beginning a transaction?


### JDBC case

With a local transaction (org.hibernate.Transaction) started, we'd delegate to the JDBC physical transaction.  Without
a local transaction, we'd be in an auto-commit case.  Do we want to support this auto-commit case?  We kind-of/sort-of
do today.

* Transaction.begin() would call:
    1. jdbcPhysicalTransaction.begin()
    2. coordinator.afterBegin()

* Transaction.commit() would call:
    1. coordinator.beforeCompletion()
    2. jdbcPhysicalTransaction.commit()
    3. coordinator.afterCompletion( true )

* Transaction.rollback() would call:
    1. jdbcPhysicalTransaction.rollback()
    2. coordinator.afterCompletion( false )


### JTA case(s)

As mentioned above, a proposed conceptual change here is to always have the JTA based transaction handling register
a JTA Synchronization and to always use that Synchronization to drive the coordinator.  Registration of
the Synchronization would still depend on the join/begin rules.

* Transaction.begin() would call:
	1. jtaPhysicalTransaction.begin()
    2. coordinator.afterBegin()

_Need to consider implicit begin (via auto-join) here too_

* Transaction.commit() would call:
    1. jtaPhysicalTransaction.commit()

* Transaction.rollback() would call:
    1. jtaPhysicalTransaction.rollback()

Note the missing coordinator calls in the commit() and rollback() JTA-based cases above.  Again this is due to moving
to always using a Synchronization to drive the coordinator.  In an effort to keep the (local) Transaction simple, we
could still make the coordinator calls from the local Transaction and simply no-op those calls in the coordinator itself
for these cases.  Yet another option would be to hide this behind a PhysicalTransaction facade that DoesTheRightThing
based on JDBC/JTA flavor; but that smells to me.



