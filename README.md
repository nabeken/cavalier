# Cavalier

`Cavarier` is a command-line tool to help database testing with snapshots taken by Amazon RDS.

## Motivation

## How `Cavarier` helps the database testing in prod?

`Cavarier` provides an automated way to
- Create a new RDS instance from a given snapshot
- Modify the instance
- Reset the master password to a temporary one
- Establish a secure connection from your local to the instance
- Launch the frontend command-line apps (e.g. `psql`) that is ensured to connects to the cloned database instance

Once you finish testing with the instance, `Cavarier` is able to remove the instance and the snapshot for your testing in prod.

Automation is a key to ensure there is no chance to introduce a human error to deal with the database in prod.

With `Cavarier`, you no longer need to specify a hostname of a database instance which is a source of terrible incidents like deleting records in prod, terminating a prod database, etc.
