# Cavalier

`Cavarier` is a command-line tool to help database testing with snapshots taken by Amazon RDS.

## Motivation

## How `Cavarier` helps the database testing in prod?

`Cavarier` provides an automated way to
- Create a new RDS instance
- Restore an RDS instance from a given snapshot
- Modify the instance
  - Disable the backup
- Reset the master password to a temporary one
- Establish a secure connection from your local to the instance
- Launch the frontend command-line apps (e.g. `psql`) that is ensured to connects to the cloned database instance

Once you finish testing with the instance, `Cavarier` is able to remove the instance and the snapshot for your testing in prod.

Automation is a key to ensure there is no chance to introduce a human error to deal with the database in prod.

With `Cavarier`, you no longer need to specify a hostname of a database instance which is a source of terrible incidents like deleting records in prod, terminating a prod database, etc.

## Connectivity to the DB instance

You should never expose your DB instance in the public. Instead, you should deploy a bastion server in your VPC and connect the DB instance through it.

If you already have a bastion server with SSH access, `Cavarier` can use it.

```
         SSH
[local] -----> [bastion] -----> [DB Instance]
```

If you already have a bastion server with AWS System Manager's Session Manager, `Cavarier` can also use it.

```
         SSH        ssm-agent
[local] -----> [SSM] <----- [bastion] -----> [DB Instance]
```

In this setup, only AWS-managed SSM endpoint is exposed to the public. The bastion can be ECS-exec.

## Recommendation

- You should prepare a dedicated VPC network and security groups other than prod setup.
- You should prepare a dedicated DB Subnet Group.
- You should use IAM Authentication.
