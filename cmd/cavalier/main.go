package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/nabeken/cavalier"
	"github.com/spf13/cobra"
)

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

	ccfg := &cavalier.Config{}

	cv := cavalier.New(
		ccfg,
		rds.NewFromConfig(cfg),
		secretsmanager.NewFromConfig(cfg),
	)

	rootCmd := rootCmdFlags(ccfg, &cobra.Command{
		Use:   "cavalier",
		Short: "cavalier is a ommand-line tool to help database testing with snapshots taken by Amazon RDS",
		Run: func(cmd *cobra.Command, args []string) {
			cmd.PrintErr("please specify a subcommand\n")
		},
	})

	snapshotCmd := snapshotCmdFlags(ccfg, &cobra.Command{
		Use:   "snapshot",
		Short: "Take a DB snapshot of a running DB Snapshot",
		Run: func(cmd *cobra.Command, args []string) {
			if err := cv.HandleSnapshot(ctx); err != nil {
				log.Fatalf("Failed to take the DB snapshot: %s", err)
			}
		},
	})

	restoreCmd := restoreCmdFlags(ccfg, &cobra.Command{
		Use:   "restore",
		Short: "Restore a DB instance from a given DB Snapshot",

		PreRun: func(cmd *cobra.Command, args []string) {
			if ccfg.SnapshotARN != "" && ccfg.SourceDBInstanceIdentifier != "" {
				cmd.PrintErr("--snapshot-arn and --source-db-instance-identifier can't be used together\n")
				os.Exit(1)
			}

			if ccfg.SnapshotARN == "" && ccfg.SourceDBInstanceIdentifier == "" {
				cmd.PrintErr("--snapshot-arn or --source-db-instance-identifier must be specified\n")
				os.Exit(1)
			}

			if ccfg.SourceDBInstanceIdentifier != "" {
				ccfg.TakeSnapshot()
			}
		},

		Run: func(cmd *cobra.Command, args []string) {
			if err := cv.HandleRestore(ctx); err != nil {
				log.Fatalf("Failed to restore the DB instance: %s", err)
			}
		},
	})

	modifyCmd := modifyCmdFlags(ccfg, &cobra.Command{
		Use:   "modify",
		Short: "Modify the existing DB instance created by the cavalier",
		Run: func(cmd *cobra.Command, args []string) {
			if err := cv.HandleModify(ctx); err != nil {
				log.Fatalf("Failed to modify the DB instance: %s", err)
			}
		},
	})

	terminateCmd := terminateCmdFlags(ccfg, &cobra.Command{
		Use:   "terminate",
		Short: "Terminate the DB instance created by the cavalier",
		Run: func(cmd *cobra.Command, args []string) {
			if err := cv.HandleTerminate(ctx); err != nil {
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
	ccfg *cavalier.Config
	cmd  *cobra.Command
}

func (fb *flagsBuilder) Build(name string) *flagsBuilder {
	ccfg := fb.ccfg
	cmd := fb.cmd

	switch name {
	case "secrets-manager-prefix":
		cmd.Flags().StringVar(
			&ccfg.SecretsManagerPrefix, name, "cavalier", "secrets manager prefix to store the master user password",
		)

	case "db-instance-identifier":
		cmd.Flags().StringVar(
			&ccfg.DBInstanceIdentifier, name, "", "DB instance identifier (required)",
		)
		cmd.MarkFlagRequired(name)

	case "source-db-instance-identifier":
		cmd.Flags().StringVar(&ccfg.SourceDBInstanceIdentifier,
			name, "", "source DB instance identifier to take snapshot (required)",
		)

	case "db-instance-class":
		cmd.Flags().StringVar(&ccfg.DBInstanceClass, name, "db.t3.medium", "DB instance class")

	case "db-parameter-group":
		cmd.Flags().StringVar(&ccfg.DBParameterGroupName, name, "", "DB parameter group")

	case "option-group":
		cmd.Flags().StringVar(&ccfg.OptionGroupName, name, "", "option group name")

	case "snapshot-arn":
		cmd.Flags().StringVar(&ccfg.SnapshotARN, name, "", "snapshot ARN to restore (required)")

	case "db-subnet-group":
		cmd.Flags().StringVar(&ccfg.DBSubnetGroupName, name, "", "DB subnet group (required)")
		cmd.MarkFlagRequired(name)

	case "vpc-security-groups":
		cmd.Flags().StringSliceVar(
			&ccfg.VPCSecurityGroupIDs, name, nil, "comma-separated VPC Security Group IDs (required)")
		cmd.MarkFlagRequired(name)

	default:
		panic(fmt.Sprintf("unknown flag: %s", name))
	}

	return fb
}

func rootCmdFlags(ccfg *cavalier.Config, c *cobra.Command) *cobra.Command {
	return (&flagsBuilder{ccfg, c}).Build("secrets-manager-prefix").cmd
}

func terminateCmdFlags(ccfg *cavalier.Config, c *cobra.Command) *cobra.Command {
	return (&flagsBuilder{ccfg, c}).Build("db-instance-identifier").cmd
}

func snapshotCmdFlags(ccfg *cavalier.Config, c *cobra.Command) *cobra.Command {
	return (&flagsBuilder{ccfg, c}).
		Build("db-instance-identifier").
		Build("source-db-instance-identifier").
		cmd
}

func restoreCmdFlags(ccfg *cavalier.Config, c *cobra.Command) *cobra.Command {
	// optional
	return (&flagsBuilder{ccfg, c}).
		Build("db-instance-identifier").
		Build("source-db-instance-identifier").
		Build("db-instance-class").
		Build("db-parameter-group").
		Build("option-group").
		Build("snapshot-arn").
		Build("db-subnet-group").
		Build("vpc-security-groups").cmd
}

func modifyCmdFlags(ccfg *cavalier.Config, c *cobra.Command) *cobra.Command {
	return (&flagsBuilder{ccfg, c}).Build("db-instance-identifier").cmd
}
