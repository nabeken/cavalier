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
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	smtypes "github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/sethvargo/go-password/password"
	"github.com/spf13/cobra"
)

type opts struct {
	SnapshotARN string

	DBInstanceClass      string
	DBSubnetGroupName    string
	DBInstanceIdentifier string
	DBParameterGroupName string

	OptionGroupName      string
	SecretsManagerPrefix string

	VPCSecurityGroupIDs []string
}

type RDSClient struct {
	RDS *rds.Client
}

type SecretsManagerClient struct {
	SM *secretsmanager.Client
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
		smc:  &SecretsManagerClient{secretsmanager.NewFromConfig(cfg)},
	}

	rootCmd := rootCmdFlags(cv, &cobra.Command{
		Use:   "cavalier",
		Short: "cavalier is a ommand-line tool to help database testing with snapshots taken by Amazon RDS",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("please specify a subcommand")
		},
	})

	restoreCmd := restoreCmdFlags(cv, &cobra.Command{
		Use:   "restore",
		Short: "Restore a DB instance from a given DB Snapshot",
		Run: func(cmd *cobra.Command, args []string) {
			if err := cv.handleRestore(ctx); err != nil {
				log.Fatalf("Failed to restore the DB instance: %s", err)
			}
		},
	})

	modifyCmd := modifyCmdFlags(cv, &cobra.Command{
		Use:   "modify",
		Short: "Modify the existing DB instance created by the cavalier",
		Run: func(cmd *cobra.Command, args []string) {
			if err := cv.handleModify(ctx); err != nil {
				log.Fatalf("Failed to modify the DB instance: %s", err)
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
	rootCmd.AddCommand(modifyCmd)

	return rootCmd.Execute()
}

type flagsBuilder struct {
	cv  *Cavalier
	cmd *cobra.Command
}

func (fb *flagsBuilder) Build(name string) *flagsBuilder {
	o := &fb.cv.opts

	switch name {
	case "secrets-manager-prefix":
		fb.cmd.Flags().StringVar(
			&o.SecretsManagerPrefix, name, "cavalier", "secrets manager prefix to store the master user password",
		)

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

func rootCmdFlags(cv *Cavalier, c *cobra.Command) *cobra.Command {
	(&flagsBuilder{cv, c}).Build("secrets-manager-prefix")

	return c
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

func modifyCmdFlags(cv *Cavalier, c *cobra.Command) *cobra.Command {
	(&flagsBuilder{cv, c}).Build("db-instance-identifier")

	return c
}

type Cavalier struct {
	opts opts
	rdsc *RDSClient
	smc  *SecretsManagerClient
}

type DBInstance struct {
	Identifier         string
	MasterUserPassword string
}

func (c *Cavalier) handleTerminate(ctx context.Context) error {
	// refuse to terminate if the instance wasn't created by the cavalier
	dbi, ok, err := c.isCreatedByCavalier(ctx)
	if err != nil {
		return err
	}

	if !ok {
		return errors.New("the specified DB instance wasn't created by the cavalier")
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

	// removing the secret for the DB instance
	if err := c.deleteMasterUserPasswordSecret(ctx, *dbi.DBInstanceIdentifier); err != nil {
		return err
	}

	log.Print("The master user password for the DB instance has been deleted.")

	return nil
}

func (c *Cavalier) isCreatedByCavalier(ctx context.Context) (types.DBInstance, bool, error) {
	resp, err := c.rdsc.RDS.DescribeDBInstances(ctx, &rds.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String(c.opts.DBInstanceIdentifier),
	})

	if err != nil {
		return types.DBInstance{}, false, fmt.Errorf("describing the DB instance: %w", err)
	}

	if len(resp.DBInstances) != 1 {
		return types.DBInstance{}, false, errors.New("zero DB instance or more than one DB instances returned")
	}

	dbi := resp.DBInstances[0]

	if !isCreatedByCavalier(dbi) {
		return types.DBInstance{}, false, nil
	}

	return dbi, true, nil
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
	log.Printf("Restoring a DB instance from '%s'...", c.opts.SnapshotARN)

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

	log.Println("Waiting for the DB instance to be up and running... It may take more than 10 minutes.")

	if err := c.checkWhetherDBInstanceAvailable(
		ctx,
		resp.DBInstance.DBInstanceIdentifier,
	); err != nil {
		return err
	}

	log.Printf("The DB instance has been created.")

	if err := c.handleModify(ctx); err != nil {
		return err
	}

	return nil
}

func (c *Cavalier) checkWhetherDBInstanceAvailable(ctx context.Context, dbID *string) error {
	waiter := rds.NewDBInstanceAvailableWaiter(c.rdsc.RDS)
	if err := waiter.Wait(
		ctx,
		&rds.DescribeDBInstancesInput{
			DBInstanceIdentifier: dbID,
		},

		30*time.Minute, // should be long enough
	); err != nil {
		return fmt.Errorf("waiting for the instance to be up and running: %w", err)
	}

	return nil
}

func (c *Cavalier) createMasterUserPasswordSecret(
	ctx context.Context,
	dbInstanceIdentifier string,
	masterUserPassword string,
) (string, error) {
	resp, err := c.smc.SM.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
		Name:         aws.String(fmt.Sprintf("%s/%s", c.opts.SecretsManagerPrefix, dbInstanceIdentifier)),
		Description:  aws.String("Randomly generated the master user password for RDS DB instance (by Cavalier)"),
		SecretString: aws.String(masterUserPassword),
	})

	if err != nil {
		return "", fmt.Errorf("creating a new master user password: %w", err)
	}

	return aws.ToString(resp.ARN), nil
}

func (c *Cavalier) deleteMasterUserPasswordSecret(
	ctx context.Context,
	dbInstanceIdentifier string,
) error {
	_, err := c.smc.SM.DeleteSecret(ctx, &secretsmanager.DeleteSecretInput{
		SecretId: aws.String(masterUserPasswordSecretName(
			c.opts.SecretsManagerPrefix,
			dbInstanceIdentifier,
		)),
		ForceDeleteWithoutRecovery: true,
	})

	if err != nil {
		return fmt.Errorf("deleting the master user password secret: %w", err)
	}

	return nil
}

func (c *Cavalier) getMasterUserPasswordSecret(
	ctx context.Context,
	dbInstanceIdentifier string,
) (string, error) {
	name := masterUserPasswordSecretName(c.opts.SecretsManagerPrefix, dbInstanceIdentifier)

	resp, err := c.smc.SM.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId: aws.String(name),
	})

	if err != nil {
		return "", fmt.Errorf("getting the master user password: %w", err)
	}

	return aws.ToString(resp.SecretString), nil
}

func (c *Cavalier) handleModify(ctx context.Context) error {
	// refuse to modify if the instance wasn't created by the cavalier
	dbi, ok, err := c.isCreatedByCavalier(ctx)
	if err != nil {
		return err
	}

	if !ok {
		return errors.New("the specified DB instance wasn't created by the cavalier")
	}

	dbID := *dbi.DBInstanceIdentifier

	// checking the status
	log.Println("Checking whether the DB instance is available...")
	if err := c.checkWhetherDBInstanceAvailable(
		ctx,
		dbi.DBInstanceIdentifier,
	); err != nil {
		return err
	}

	log.Printf("Generating a new master user password...")

	mupw, err := generateMasterUserPassword()
	if err != nil {
		return fmt.Errorf("generating the master user password: %w", err)
	}

	mupwARN, cerr := c.createMasterUserPasswordSecret(ctx, dbID, mupw)
	if cerr != nil {
		var smerr *smtypes.ResourceExistsException
		if !errors.As(cerr, &smerr) {
			return fmt.Errorf("creating the master user password: %w", cerr)
		}

		log.Print("The master user password already exists. Reusing it.")

		existingPassword, err := c.getMasterUserPasswordSecret(ctx, dbID)
		if err != nil {
			return fmt.Errorf("gettign the existing master user password: %w", err)
		}

		mupw = existingPassword
	} else {
		log.Printf("A new master user password has been generated on %s", mupwARN)
	}

	log.Printf("Modifying for the DB instance of %s to for the testing...", dbID)

	if _, err := c.rdsc.RDS.ModifyDBInstance(ctx, &rds.ModifyDBInstanceInput{
		DBInstanceIdentifier:  aws.String(dbID),
		ApplyImmediately:      true,
		BackupRetentionPeriod: aws.Int32(0),
		MasterUserPassword:    aws.String(mupw),
	}); err != nil {
		return fmt.Errorf("modifying the DB instance: %w", err)
	}

	time.Sleep(10 * time.Second)

	if err := c.checkWhetherDBInstanceAvailable(
		ctx,
		dbi.DBInstanceIdentifier,
	); err != nil {
		return err
	}

	log.Printf("The DB instance has been modified.")

	return nil
}

func stringOrNil(v string) *string {
	if v == "" {
		return nil
	}
	return aws.String(v)
}

// https://docs.aws.amazon.com/AmazonRDS/latest/APIReference/API_ModifyDBInstance.html
const masterUserPasswordSymbols = "~!#$%^&*()_+`-={}|[]\\:<>?,."

var masterUserPasswordGen *password.Generator

func init() {
	gen, err := password.NewGenerator(&password.GeneratorInput{
		Symbols: masterUserPasswordSymbols,
	})
	if err != nil {
		panic(fmt.Errorf("initializing the master user password generator: %w", err))
	}

	masterUserPasswordGen = gen
}

func generateMasterUserPassword() (string, error) {
	// set to the maximum length that MySQL can accept
	return masterUserPasswordGen.Generate(41, 10, 10, false, false)
}

func masterUserPasswordSecretName(prefix, name string) string {
	return fmt.Sprintf("%s/%s", prefix, name)
}
