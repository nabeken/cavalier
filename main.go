package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	smtypes "github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/aws/smithy-go"
	"github.com/sethvargo/go-password/password"
	"github.com/spf13/cobra"
)

type opts struct {
	SourceDBInstanceIdentifier string

	SnapshotARN string

	DBInstanceClass      string
	DBSubnetGroupName    string
	DBInstanceIdentifier string
	DBParameterGroupName string

	OptionGroupName      string
	SecretsManagerPrefix string

	VPCSecurityGroupIDs []string

	takeSnapshot bool
}

type RDSClient struct {
	RDS *rds.Client
}

func (c *RDSClient) DescribeDBSnapshotByIdentifier(ctx context.Context, dbi string) (types.DBSnapshot, error) {
	var zero types.DBSnapshot

	p := rds.NewDescribeDBSnapshotsPaginator(c.RDS, &rds.DescribeDBSnapshotsInput{
		DBSnapshotIdentifier: aws.String(dbSnapshotName(dbi)),
		SnapshotType:         aws.String("manual"),
	})

	for p.HasMorePages() {
		resp, err := p.NextPage(ctx)
		if err != nil {
			return zero, fmt.Errorf("describing the DB snapshot: %w", err)
		}

		for _, s := range resp.DBSnapshots {
			if isSnapshotCreatedByCavalier(dbi, s) {
				return s, nil
			}
		}
	}

	return zero, errors.New("no corresponding the DB snapshot")
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
			cmd.PrintErr("please specify a subcommand")
		},
	})

	snapshotCmd := snapshotCmdFlags(cv, &cobra.Command{
		Use:   "snapshot",
		Short: "Take a DB snapshot of a running DB Snapshot",
		Run: func(cmd *cobra.Command, args []string) {
			if err := cv.handleSnapshot(ctx); err != nil {
				log.Fatalf("Failed to take the DB snapshot: %s", err)
			}
		},
	})

	restoreCmd := restoreCmdFlags(cv, &cobra.Command{
		Use:   "restore",
		Short: "Restore a DB instance from a given DB Snapshot",

		PreRun: func(cmd *cobra.Command, args []string) {
			if cv.opts.SnapshotARN != "" && cv.opts.SourceDBInstanceIdentifier != "" {
				cmd.PrintErr("--snapshot-arn and --source-db-instance-identifier can't be used together\n")
				os.Exit(1)
			}

			if cv.opts.SnapshotARN == "" && cv.opts.SourceDBInstanceIdentifier == "" {
				cmd.PrintErr("--snapshot-arn or --source-db-instance-identifier must be specified\n")
				os.Exit(1)
			}

			if cv.opts.SourceDBInstanceIdentifier != "" {
				cv.opts.takeSnapshot = true
			}
		},

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
	rootCmd.AddCommand(snapshotCmd)

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
	case "source-db-instance-identifier":
		fb.cmd.Flags().StringVar(&o.SourceDBInstanceIdentifier,
			name, "", "source DB instance identifier to take snapshot (required)",
		)

	default:
		panic(fmt.Sprintf("unknown flag: %s", name))
	}

	return fb
}

func rootCmdFlags(cv *Cavalier, c *cobra.Command) *cobra.Command {
	return (&flagsBuilder{cv, c}).Build("secrets-manager-prefix").cmd
}

func terminateCmdFlags(cv *Cavalier, c *cobra.Command) *cobra.Command {
	return (&flagsBuilder{cv, c}).Build("db-instance-identifier").cmd
}

func snapshotCmdFlags(cv *Cavalier, c *cobra.Command) *cobra.Command {
	return (&flagsBuilder{cv, c}).
		Build("db-instance-identifier").
		Build("source-db-instance-identifier").
		cmd
}

func restoreCmdFlags(cv *Cavalier, c *cobra.Command) *cobra.Command {
	o := &cv.opts

	// optional
	c.Flags().StringVar(&o.DBInstanceClass, "db-instance-class", "db.t3.medium", "DB instance class")
	c.Flags().StringVar(&o.DBParameterGroupName, "db-parameter-group", "", "DB parameter group")
	c.Flags().StringVar(&o.OptionGroupName, "option-group", "", "option group name")
	c.Flags().StringVar(&o.SnapshotARN, "snapshot-arn", "", "snapshot ARN to restore (required)")

	c.Flags().StringVar(&o.DBSubnetGroupName, "db-subnet-group", "", "DB subnet group (required)")
	c.MarkFlagRequired("db-subnet-group")

	(&flagsBuilder{cv, c}).
		Build("db-instance-identifier").
		Build("source-db-instance-identifier")

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

func IsDBDeleted(err error) bool {
	var notFoundErr *types.DBInstanceNotFoundFault
	return errors.As(err, &notFoundErr)
}

func (c *Cavalier) deleteDBInstance(ctx context.Context, dbInstanceIdentifier string) error {
	_, err := c.rdsc.RDS.DeleteDBInstance(ctx, &rds.DeleteDBInstanceInput{
		DBInstanceIdentifier:   aws.String(dbInstanceIdentifier),
		DeleteAutomatedBackups: aws.Bool(true),
		SkipFinalSnapshot:      true,
	})

	var notFoundErr *types.DBInstanceNotFoundFault
	if errors.As(err, &notFoundErr) {
		log.Printf("The DB instance is already deleted.")
		return nil
	}

	var invalidStateErr *types.InvalidDBInstanceStateFault
	if err != nil && !errors.As(err, &invalidStateErr) {
		return fmt.Errorf("deleting the DB instance: %w", err)
	}

	log.Printf("Waiting for the DB instance to be deleted...")

	time.Sleep(10 * time.Second)

	waiter := rds.NewDBInstanceDeletedWaiter(c.rdsc.RDS, dbInstanceDeletedWaiterOption)

	if err := waiter.Wait(
		ctx,
		&rds.DescribeDBInstancesInput{
			DBInstanceIdentifier: aws.String(dbInstanceIdentifier),
		},

		30*time.Minute, // should be long enough
	); err != nil {
		return fmt.Errorf("waiting for the instance to be deleted: %w", err)
	}

	return nil
}

func (c *Cavalier) handleTerminate(ctx context.Context) error {
	var dbAlreadyDeleted bool

	// refuse to terminate if the instance wasn't created by the cavalier
	dbi, ok, err := c.isCreatedByCavalier(ctx)
	if err != nil {
		if IsDBDeleted(err) {
			dbAlreadyDeleted = true
		} else {
			return err
		}
	}

	if !dbAlreadyDeleted && !ok {
		return errors.New("the specified DB instance wasn't created by the cavalier")
	}

	if !dbAlreadyDeleted {
		log.Printf("Terminating the DB instance '%s'...", c.opts.DBInstanceIdentifier)

		if err := c.deleteDBInstance(ctx, c.opts.DBInstanceIdentifier); err != nil {
			return err
		}

		log.Printf("The DB instance '%s' has been terminated", c.opts.DBInstanceIdentifier)
	}

	// removing the secret for the DB instance
	if err := c.deleteMasterUserPasswordSecret(ctx, c.opts.DBInstanceIdentifier); err != nil {
		return err
	}

	log.Print("The master user password for the DB instance has been deleted.")

	// delete the corresponding snapshot if exists
	dbs, err := c.rdsc.DescribeDBSnapshotByIdentifier(ctx, c.opts.DBInstanceIdentifier)
	if err != nil {
		return err
	}

	if doesUseSnapshotCreatedByCavalier(dbi.TagList) {
		log.Print("Removing the corresponding DB snapshot...")

		_, err := c.rdsc.RDS.DeleteDBSnapshot(ctx, &rds.DeleteDBSnapshotInput{
			DBSnapshotIdentifier: dbs.DBSnapshotIdentifier,
		})

		if err != nil {
			return fmt.Errorf("removing the corresponding DB snapshot: %w", err)
		}

		log.Print("The corresponding DB snapshot has been removed.")
	}

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

func isSnapshotCreatedByCavalier(dbi string, dbs types.DBSnapshot) bool {
	for _, t := range dbs.TagList {
		if aws.ToString(t.Key) != "CAVALIER_DB_INSTANCE_IDENTIFIER" {
			continue
		}

		v := aws.ToString(t.Value)
		if v == dbi {
			return true
		}
	}

	return false
}

func doesUseSnapshotCreatedByCavalier(tags []types.Tag) bool {
	for _, t := range tags {
		if aws.ToString(t.Key) != "USE_SNAPSHOT_CREATED_BY_CAVALIER" {
			continue
		}

		v := aws.ToString(t.Value)
		ok, _ := strconv.ParseBool(v)

		return ok
	}

	return false
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

func dbSnapshotName(dbi string) string {
	return fmt.Sprintf("%s-cavalier", dbi)
}

func (c *Cavalier) handleSnapshot(ctx context.Context) error {
	log.Printf("Taking the DB snapshot of '%s'...", c.opts.SourceDBInstanceIdentifier)

	_, err := c.rdsc.RDS.CreateDBSnapshot(ctx, &rds.CreateDBSnapshotInput{
		DBInstanceIdentifier: aws.String(c.opts.SourceDBInstanceIdentifier),
		DBSnapshotIdentifier: aws.String(dbSnapshotName(c.opts.DBInstanceIdentifier)),

		Tags: []types.Tag{
			{
				Key:   aws.String("CAVALIER_DB_INSTANCE_IDENTIFIER"),
				Value: aws.String(c.opts.DBInstanceIdentifier),
			},
		},
	})

	if err != nil {
		return fmt.Errorf("creating the DB snapshot: %w", err)
	}

	log.Print("Waiting for the snapshot to be available...")

	if err := c.checkWhetherDBSnapshotAvailable(
		ctx,
		c.opts.DBInstanceIdentifier,
	); err != nil {
		return err
	}

	log.Printf("The DB snapshot has been created.")

	return nil
}

func (c *Cavalier) handleRestore(ctx context.Context) error {
	snapshotARN := c.opts.SnapshotARN

	if c.opts.takeSnapshot {
		if err := c.handleSnapshot(ctx); err != nil {
			return err
		}

		// get the corresponding snapshot ARN
		dbs, err := c.rdsc.DescribeDBSnapshotByIdentifier(
			ctx,
			c.opts.DBInstanceIdentifier,
		)
		if err != nil {
			return err
		}

		snapshotARN = aws.ToString(dbs.DBSnapshotArn)
	}

	tags := []types.Tag{
		{
			Key:   aws.String("CREATED_BY_CAVALIER"),
			Value: aws.String("true"),
		},
	}

	if c.opts.takeSnapshot {
		tags = append(tags, types.Tag{
			Key:   aws.String("USE_SNAPSHOT_CREATED_BY_CAVALIER"),
			Value: aws.String("true"),
		})
	}

	log.Printf("Restoring a DB instance from '%s'...", snapshotARN)

	resp, err := c.rdsc.RDS.RestoreDBInstanceFromDBSnapshot(
		ctx,
		&rds.RestoreDBInstanceFromDBSnapshotInput{
			DBSnapshotIdentifier: aws.String(snapshotARN),
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

			Tags: tags,
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

func (c *Cavalier) checkWhetherDBSnapshotAvailable(ctx context.Context, dbID string) error {
	waiter := rds.NewDBSnapshotAvailableWaiter(c.rdsc.RDS, dbSnapshotAvailableWaiterOption)
	if err := waiter.Wait(
		ctx,
		&rds.DescribeDBSnapshotsInput{
			DBSnapshotIdentifier: aws.String(dbSnapshotName(dbID)),
		},

		30*time.Minute, // should be long enough
	); err != nil {
		return fmt.Errorf("waiting for the instance to be up and running: %w", err)
	}

	return nil
}

func (c *Cavalier) checkWhetherDBInstanceAvailable(ctx context.Context, dbID *string) error {
	waiter := rds.NewDBInstanceAvailableWaiter(c.rdsc.RDS, dbInstanceAvailableWaiterOption)
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

func dbSnapshotAvailableWaiterOption(opts *rds.DBSnapshotAvailableWaiterOptions) {
	opts.MinDelay = 30 * time.Second
	opts.MaxDelay = opts.MaxDelay

	origRetryable := opts.Retryable
	setCustomRDSRetryable(origRetryable, &opts.Retryable)
}

func dbInstanceAvailableWaiterOption(opts *rds.DBInstanceAvailableWaiterOptions) {
	opts.MinDelay = 30 * time.Second
	opts.MaxDelay = opts.MaxDelay

	origRetryable := opts.Retryable
	setCustomRDSRetryable(origRetryable, &opts.Retryable)
}

func dbInstanceDeletedWaiterOption(opts *rds.DBInstanceDeletedWaiterOptions) {
	opts.MinDelay = 30 * time.Second
	opts.MaxDelay = opts.MaxDelay

	origRetryable := opts.Retryable
	setCustomRDSRetryable(origRetryable, &opts.Retryable)
}

func setCustomRDSRetryable[In any, Out any](
	origFn func(
		context.Context,
		*In,
		*Out,
		error,
	) (bool, error),
	fp *(func(
		context.Context,
		*In,
		*Out,
		error,
	) (bool, error)),
) {
	*fp = func(
		ctx context.Context,
		input *In,
		output *Out,
		err error,
	) (bool, error) {
		ok, rerr := waiterRetryable(ctx, input, output, err)
		if !errors.Is(rerr, errSkipRetrable) {
			return ok, err
		}

		return origFn(ctx, input, output, err)
	}
}

var errSkipRetrable = errors.New("cavalier: skip retryable")

func waiterRetryable[In any, Out any](
	ctx context.Context,
	_ *In,
	_ *Out,
	err error,
) (bool, error) {
	// return true if no decision will be made here
	if err == nil {
		return false, errSkipRetrable
	}

	var apiErr smithy.APIError
	ok := errors.As(err, &apiErr)
	if !ok {
		return false, errSkipRetrable
	}

	if apiErr.ErrorCode() == "ExpiredToken" {
		return false, err
	}

	return false, errSkipRetrable
}
