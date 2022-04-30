package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/spf13/cobra"
)

type opts struct {
	SnapshotARN string

	DBInstanceClass      string
	DBSubnetGroupName    string
	DBInstanceIdentifier string
	DBParameterGroupName string

	OptionGroupName string

	VPCSecurityGroupIDs []string
}

type RDSClient struct {
	RDS *rds.Client
}

func main() {
	if err := realmain(); err != nil {
		log.Fatal(err)
	}
}

func realmain() error {
	ctx := context.Background()

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("loading SDK config, %w", err)
	}

	cv := &Cavalier{
		rdsc: &RDSClient{RDS: rds.NewFromConfig(cfg)},
	}

	rootCmd := &cobra.Command{
		Use:   "cavalier",
		Short: "cavalier is a ommand-line tool to help database testing with snapshots taken by Amazon RDS",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("please specify a subcommand")
		},
	}

	restoreCmd := restoreCmdFlags(cv, &cobra.Command{
		Use:   "restore",
		Short: "Restore a DB instance from a given DB Snapshot",
		Run: func(cmd *cobra.Command, args []string) {
			if err := cv.handleRestore(ctx); err != nil {
				log.Fatalf("Failed to restore the DB instance: %s", err)
			}
		},
	})

	rootCmd.AddCommand(restoreCmd)

	return rootCmd.Execute()
}

func restoreCmdFlags(cv *Cavalier, c *cobra.Command) *cobra.Command {
	o := &cv.opts

	// optional
	c.Flags().StringVar(&o.DBInstanceClass, "db-instance-class", "db.t3.medium", "DB instance class")
	c.Flags().StringVar(&o.DBParameterGroupName, "db-parameter-group", "", "DB parameter group")
	c.Flags().StringVar(&o.OptionGroupName, "option-group", "", "option group name")

	// required
	c.Flags().StringVar(&o.SnapshotARN, "snapshot-arn", "", "snapshot ARN to restore (required)")
	c.MarkFlagRequired("snapshot-arn")

	c.Flags().StringVar(&o.DBSubnetGroupName, "db-subnet-group", "", "DB subnet group (required)")
	c.MarkFlagRequired("db-subnet-group")

	c.Flags().StringVar(&o.DBInstanceIdentifier, "db-instance-identifier", "", "DB instance identifier (required)")
	c.MarkFlagRequired("db-instance-identifier")

	c.Flags().StringSliceVar(&o.VPCSecurityGroupIDs, "vpc-security-groups", nil, "comma-separated VPC Security Group IDs (required)")
	c.MarkFlagRequired("vpc-security-groups")

	return c
}

type Cavalier struct {
	opts opts
	rdsc *RDSClient
}

func (c *Cavalier) handleRestore(ctx context.Context) error {
	log.Printf("Restoring a DB Instance from '%s'...", c.opts.SnapshotARN)

	resp, err := c.rdsc.RDS.RestoreDBInstanceFromDBSnapshot(
		ctx,
		&rds.RestoreDBInstanceFromDBSnapshotInput{
			DBSnapshotIdentifier: aws.String(c.opts.SnapshotARN),
			DBSubnetGroupName:    aws.String(c.opts.DBSubnetGroupName),
			VpcSecurityGroupIds:  c.opts.VPCSecurityGroupIDs,
			DBInstanceClass:      aws.String(c.opts.DBInstanceClass),
			DBInstanceIdentifier: aws.String(c.opts.DBInstanceIdentifier),

			DBParameterGroupName: stringOrNil(c.opts.DBParameterGroupName),
			OptionGroupName:      stringOrNil(c.opts.OptionGroupName),

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

	waiter := rds.NewDBInstanceAvailableWaiter(c.rdsc.RDS)
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

func stringOrNil(v string) *string {
	if v == "" {
		return nil
	}
	return aws.String(v)
}
