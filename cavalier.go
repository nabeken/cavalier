package cavalier

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	smtypes "github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/aws/smithy-go"
	"github.com/sethvargo/go-password/password"
)

type rdsClient struct {
	RDS RDSClient
}

func (c *rdsClient) DescribeDBSnapshotByIdentifier(ctx context.Context, dbi string) (types.DBSnapshot, error) {
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

type Config struct {
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

func (c *Config) TakeSnapshot() *Config {
	c.takeSnapshot = true
	return c
}

type Cavalier struct {
	cfg *Config

	rdsc *rdsClient
	smc  SecretsManagerClient
}

func New(
	cfg *Config,
	rdsc RDSClient,
	smc SecretsManagerClient,
) *Cavalier {
	return &Cavalier{
		cfg:  cfg,
		rdsc: &rdsClient{rdsc},
		smc:  smc,
	}
}

type DBInstance struct {
	Identifier         string
	MasterUserPassword string
}

func IsDBInstanceNotFound(err error) bool {
	var notFoundErr *types.DBInstanceNotFoundFault
	return errors.As(err, &notFoundErr)
}

func IsDBSnapshotNotFound(err error) bool {
	var notFoundErr *types.DBSnapshotNotFoundFault
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

func (c *Cavalier) HandleTerminate(ctx context.Context) error {
	var dbAlreadyDeleted bool

	// refuse to terminate if the instance wasn't created by the cavalier
	_, ok, err := c.isCreatedByCavalier(ctx)
	if err != nil {
		if IsDBInstanceNotFound(err) {
			dbAlreadyDeleted = true
		} else {
			return err
		}
	}

	if !dbAlreadyDeleted && !ok {
		return errors.New("the specified DB instance wasn't created by the cavalier")
	}

	if !dbAlreadyDeleted {
		log.Printf("Terminating the DB instance '%s'...", c.cfg.DBInstanceIdentifier)

		if err := c.deleteDBInstance(ctx, c.cfg.DBInstanceIdentifier); err != nil {
			return err
		}

		log.Printf("The DB instance '%s' has been terminated", c.cfg.DBInstanceIdentifier)
	}

	// removing the secret for the DB instance
	if err := c.deleteMasterUserPasswordSecret(ctx, c.cfg.DBInstanceIdentifier); err != nil {
		return err
	}

	log.Print("The master user password for the DB instance has been deleted.")

	// delete the corresponding snapshot if exists
	dbs, err := c.rdsc.DescribeDBSnapshotByIdentifier(ctx, c.cfg.DBInstanceIdentifier)
	if err != nil {
		if !IsDBSnapshotNotFound(err) {
			return err
		}
	}

	if isSnapshotCreatedByCavalier(c.cfg.DBInstanceIdentifier, dbs) {
		log.Print("Removing the corresponding DB snapshot...")

		_, err := c.rdsc.RDS.DeleteDBSnapshot(ctx, &rds.DeleteDBSnapshotInput{
			DBSnapshotIdentifier: dbs.DBSnapshotIdentifier,
		})

		if err != nil {
			return fmt.Errorf("removing the corresponding DB snapshot: %w", err)
		}

		log.Print("The corresponding DB snapshot has been removed.")
	} else {
		log.Print("There is no corresponding DB snapshot.")
	}

	return nil
}

func (c *Cavalier) isCreatedByCavalier(ctx context.Context) (types.DBInstance, bool, error) {
	resp, err := c.rdsc.RDS.DescribeDBInstances(ctx, &rds.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String(c.cfg.DBInstanceIdentifier),
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

func (c *Cavalier) HandleSnapshot(ctx context.Context) error {
	log.Printf("Taking the DB snapshot of '%s'...", c.cfg.SourceDBInstanceIdentifier)

	_, err := c.rdsc.RDS.CreateDBSnapshot(ctx, &rds.CreateDBSnapshotInput{
		DBInstanceIdentifier: aws.String(c.cfg.SourceDBInstanceIdentifier),
		DBSnapshotIdentifier: aws.String(dbSnapshotName(c.cfg.DBInstanceIdentifier)),

		Tags: []types.Tag{
			{
				Key:   aws.String("CAVALIER_DB_INSTANCE_IDENTIFIER"),
				Value: aws.String(c.cfg.DBInstanceIdentifier),
			},
		},
	})

	if err != nil {
		return fmt.Errorf("creating the DB snapshot: %w", err)
	}

	log.Print("Waiting for the snapshot to be available...")

	if err := c.checkWhetherDBSnapshotAvailable(
		ctx,
		c.cfg.DBInstanceIdentifier,
	); err != nil {
		return err
	}

	log.Printf("The DB snapshot has been created.")

	return nil
}

func (c *Cavalier) HandleRestore(ctx context.Context) error {
	snapshotARN := c.cfg.SnapshotARN

	if c.cfg.takeSnapshot {
		if err := c.HandleSnapshot(ctx); err != nil {
			return err
		}

		// get the corresponding snapshot ARN
		dbs, err := c.rdsc.DescribeDBSnapshotByIdentifier(
			ctx,
			c.cfg.DBInstanceIdentifier,
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

	if c.cfg.takeSnapshot {
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
			DBSubnetGroupName:    aws.String(c.cfg.DBSubnetGroupName),
			VpcSecurityGroupIds:  c.cfg.VPCSecurityGroupIDs,
			DBInstanceClass:      aws.String(c.cfg.DBInstanceClass),
			DBInstanceIdentifier: aws.String(c.cfg.DBInstanceIdentifier),

			DBParameterGroupName: stringOrNil(c.cfg.DBParameterGroupName),
			OptionGroupName:      stringOrNil(c.cfg.OptionGroupName),

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

	if err := c.HandleModify(ctx); err != nil {
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
	resp, err := c.smc.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
		Name:         aws.String(fmt.Sprintf("%s/%s", c.cfg.SecretsManagerPrefix, dbInstanceIdentifier)),
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
	_, err := c.smc.DeleteSecret(ctx, &secretsmanager.DeleteSecretInput{
		SecretId: aws.String(masterUserPasswordSecretName(
			c.cfg.SecretsManagerPrefix,
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
	name := masterUserPasswordSecretName(c.cfg.SecretsManagerPrefix, dbInstanceIdentifier)

	resp, err := c.smc.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId: aws.String(name),
	})

	if err != nil {
		return "", fmt.Errorf("getting the master user password: %w", err)
	}

	return aws.ToString(resp.SecretString), nil
}

func (c *Cavalier) HandleModify(ctx context.Context) error {
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
		log.Printf("A new master user password has been saved on %s", mupwARN)
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

	time.Sleep(30 * time.Second)

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
