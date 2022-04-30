package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"
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

	terminateCmd := terminateCmdFlags(cv, &cobra.Command{
		Use:   "terminate",
		Short: "Terminate the DB instance created by the cavalier",
		Run: func(cmd *cobra.Command, args []string) {
			if err := cv.handleTerminate(ctx); err != nil {
				log.Fatalf("Failed to restore the DB instance: %s", err)
			}
		},
	})

	rootCmd.AddCommand(restoreCmd)
	rootCmd.AddCommand(terminateCmd)

	return rootCmd.Execute()
}

type flagsBuilder struct {
	cv  *Cavalier
	cmd *cobra.Command
}

func (fb *flagsBuilder) Build(name string) *flagsBuilder {
	o := &fb.cv.opts

	switch name {
	case "db-instance-identifier":
		fb.cmd.Flags().StringVar(
			&o.DBInstanceIdentifier, name, "", "DB instance identifier (required)",
		)

		fb.cmd.MarkFlagRequired(name)

	default:
		panic(fmt.Sprintf("unknown flag: %s", name))
	}

	return fb
}

func terminateCmdFlags(cv *Cavalier, c *cobra.Command) *cobra.Command {
	(&flagsBuilder{cv, c}).Build("db-instance-identifier")

	return c
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

	(&flagsBuilder{cv, c}).Build("db-instance-identifier")

	c.Flags().StringSliceVar(&o.VPCSecurityGroupIDs, "vpc-security-groups", nil, "comma-separated VPC Security Group IDs (required)")
	c.MarkFlagRequired("vpc-security-groups")

	return c
}

type Cavalier struct {
	opts opts
	rdsc *RDSClient
}

type DBInstance struct {
	Identifier string
}

func (c *Cavalier) handleTerminate(ctx context.Context) error {
	// refuse to terminate if the instance wasn't created by the cavalier
	resp, err := c.rdsc.RDS.DescribeDBInstances(ctx, &rds.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String(c.opts.DBInstanceIdentifier),
	})

	if err != nil {
		return fmt.Errorf("describing the DB instance: %w", err)
	}

	if len(resp.DBInstances) != 1 {
		return errors.New("zero DB instance or more than one DB instances returned")
	}

	dbi := resp.DBInstances[0]

	if !isCreatedByCavalier(dbi) {
		return errors.New("the specified DB instance is NOT created by the cavalier")
	}

	log.Printf("Terminating the DB instance '%s'...", c.opts.DBInstanceIdentifier)

	if _, err = c.rdsc.RDS.DeleteDBInstance(ctx, &rds.DeleteDBInstanceInput{
		DBInstanceIdentifier:   dbi.DBInstanceIdentifier,
		DeleteAutomatedBackups: aws.Bool(true),
		SkipFinalSnapshot:      true,
	}); err != nil {
		var dberr *types.InvalidDBInstanceStateFault
		if !errors.As(err, &dberr) {
			return fmt.Errorf("deleting the DB instance: %w", err)
		}

		log.Printf("The DB instance is being deleted...")
	}

	waiter := rds.NewDBInstanceDeletedWaiter(c.rdsc.RDS)

	log.Printf("Waiting for the DB instance to be deleted...")

	if err := waiter.Wait(
		ctx,
		&rds.DescribeDBInstancesInput{
			DBInstanceIdentifier: dbi.DBInstanceIdentifier,
		},

		30*time.Minute, // should be long enough
	); err != nil {
		return fmt.Errorf("waiting for the instance to be deleted: %w", err)
	}

	log.Printf("The DB instance '%s' has been terminated", *dbi.DBInstanceIdentifier)

	return nil
}

func isCreatedByCavalier(dbi types.DBInstance) bool {
	for _, t := range dbi.TagList {
		if aws.ToString(t.Key) != "CREATED_BY_CAVALIER" {
			continue
		}

		v := aws.ToString(t.Value)
		ok, _ := strconv.ParseBool(v)

		return ok
	}

	return false
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

			Tags: []types.Tag{
				{
					Key:   aws.String("CREATED_BY_CAVALIER"),
					Value: aws.String("true"),
				},
			},
		},
	)
	if err != nil {
		return fmt.Errorf("restoring the db instance: %w", err)
	}

	log.Println("Waiting for the DB Instance to be up and running... It may take more than 10 minutes.")

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

	dbInstance := DBInstance{
		Identifier: *resp.DBInstance.DBInstanceIdentifier,
	}

	if err := c.modifyDBInstance(ctx, dbInstance); err != nil {
		return err
	}

	return nil
}

func (c *Cavalier) modifyDBInstance(ctx context.Context, dbInstance DBInstance) error {
	log.Printf("Modifying for the DB Instance of %s to for the testing...", dbInstance.Identifier)

	if _, err := c.rdsc.RDS.ModifyDBInstance(ctx, &rds.ModifyDBInstanceInput{
		DBInstanceIdentifier:  aws.String(dbInstance.Identifier),
		ApplyImmediately:      true,
		BackupRetentionPeriod: aws.Int32(0),
	}); err != nil {
		return fmt.Errorf("modifying the DB instance: %w", err)
	}

	return nil
}

func stringOrNil(v string) *string {
	if v == "" {
		return nil
	}
	return aws.String(v)
}
