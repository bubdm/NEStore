# NEStore

.**N**ET **E**vent **Store**

## Introduction

NEStore is an event store for .NET Framework. An event store is where events are stored when using the Event Sourcing pattern.

But what is the Event Sourcing pattern? Using [Martin Fowler's words](http://martinfowler.com/eaaDev/EventSourcing.html):

> We can query an application's state to find out the current state of the world, and this answers many questions. However there are times when we don't just want to see where we are, we also want to know how we got there.
Event Sourcing ensures that all changes to application state are stored as a sequence of events. Not just can we query these events, we can also use the event log to reconstruct past states, and as a foundation to automatically adjust the state to cope with retroactive changes.

## Requirements

NEStore is written with .NET Framework 4.5 and write the actual data inside a MongoDb database (>= 3.0).

## Usage
 
## Main concepts

TODO

- Buckets (bounded context)
- Commits
- Commits revision
- Stream (aggregate)
- Stream revision
- Writing concurrency (optimistic lock)
- Retry logic
- Two phase commit (no transaction)
- Dispatching events and projections (idempotency)
- Handle undispatched events
- Rebuilding projections
- Relation with CQRS
- External system notifications

## Example

## References

- [Event Store](https://geteventstore.com/) : probably the best event store database
- [NEventStore](http://neventstore.org/) : another very popular event store database for .NET from where we have taken many concepts and ideas
- http://martinfowler.com/eaaDev/EventSourcing.html
- https://msdn.microsoft.com/en-us/library/dn589792.aspx
- 
