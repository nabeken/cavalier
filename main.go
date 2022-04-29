package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/rds"
)

type stringListOpt []string

func (o stringListOpt) String() string {
	return fmt.Sprint("%v", ([]string)(o))
}

func (o *stringListOpt) Set(v string) error {
	*o = append(*o, v)
	return nil
}

type opts struct {
	DBInstanceClass     string
	SnapshotARN         string
	DBSubnetGroupName   string
	VPCSecurityGroupIDs stringListOpt
}

type RDSClient struct {
	RDS *rds.Client
}

func main() {
	if err := realmain(os.Args); err != nil {
		log.Fatal(err)
	}
}

func realmain(args []string) error {
	fmt.Println("Hello, cavalier")

	ctx := context.Background()

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("loading SDK config, %w", err)
	}

	opts := &opts{}

	f := flag.NewFlagSet(args[0], flag.ExitOnError)
	f.StringVar(&opts.SnapshotARN, "snapshot-arn", "", "specify an ARN of the snapshot to restore")
	f.StringVar(&opts.DBSubnetGroupName, "db-subnet-group-name", "", "specify a DB subnet group name")
	f.StringVar(&opts.DBInstanceClass, "db-instance-class", "db.t3.medium", "specify a DB instance class")
	f.Var(&opts.VPCSecurityGroupIDs, "vpc-security-group-id", "specify a security group id")

	if err := f.Parse(args[1:]); err != nil {
		return err
	}

	if opts.SnapshotARN == "" {
		return fmt.Errorf("no snapshot-arn is specified")
	}
	if opts.DBSubnetGroupName == "" {
		return fmt.Errorf("no db-subnet-group-name is specified")
	}
	if len(opts.VPCSecurityGroupIDs) == 0 {
		return fmt.Errorf("no vpc-security-group-id is specified")
	}

	rdsc := &RDSClient{RDS: rds.NewFromConfig(cfg)}

	log.Printf("Restoring a DB Instance from '%s'...", opts.SnapshotARN)

	resp, err := rdsc.RDS.RestoreDBInstanceFromDBSnapshot(
		ctx,
		&rds.RestoreDBInstanceFromDBSnapshotInput{
			DBSnapshotIdentifier: aws.String(opts.SnapshotARN),
			DBSubnetGroupName:    aws.String(opts.DBSubnetGroupName),
			VpcSecurityGroupIds:  opts.VPCSecurityGroupIDs,
			DBInstanceClass:      aws.String(opts.DBInstanceClass),

			// FIXME:
			DBInstanceIdentifier: aws.String("test"),

			//DBParameterGroupName
			//OptionGroupName

			EnableIAMDatabaseAuthentication: aws.Bool(true),
			PubliclyAccessible:              aws.Bool(false),
			AutoMinorVersionUpgrade:         aws.Bool(false),
			MultiAZ:                         aws.Bool(false),
		},
	)
	if err != nil {
		return fmt.Errorf("restoring the db instance: %w", err)
	}

	log.Println("Waiting for the DB Instance to be up and running...")

	waiter := rds.NewDBInstanceAvailableWaiter(rdsc.RDS)
	if err := waiter.Wait(
		ctx,
		&rds.DescribeDBInstancesInput{
			DBInstanceIdentifier: resp.DBInstance.DBInstanceIdentifier,
		},

		30*time.Minute, // should be long enough
	); err != nil {
		return fmt.Errorf("waiting for the instance to be up and running: %w", err)
	}

	log.Printf("The DB Instance has been created.")
	log.Printf("DB Instance: %v\n", resp.DBInstance)

	return nil
}
