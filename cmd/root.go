package cmd

import (
	"github.com/budiariyanto/mbludak/producer"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"log"
	"os"
)

var rootCmd = &cobra.Command{
	Use:   "mbludak",
	Short: "Kafka load testing tool",
	Long: `Kafka load testing tool`,
	Run: func(cmd *cobra.Command, args []string) {
		flags := cmd.Flags()
		appContext := getAppContext(flags)
		result, err := producer.Dispatch(appContext)
		if err != nil {
			log.Fatalln(err)
		}

		log.Printf("Total: %d, Success: %d, Failed: %d\n", result.Total, result.SuccessCount, result.FailedCount)
	},
}

func flagErrCheck(err error) {
	if err != nil {
		panic(err)
	}
}

func getAppContext(flags *pflag.FlagSet) producer.AppContext {
	brokers, err := flags.GetString("servers")
	flagErrCheck(err)
	topic, err := flags.GetString("topic")
	flagErrCheck(err)
	file, err := flags.GetString("file")
	flagErrCheck(err)
	format, err := flags.GetString("format")
	flagErrCheck(err)
	//threads, err := flags.GetInt64("threads")
	//flagErrCheck(err)
	//duration, err := flags.GetDuration("duration")
	//flagErrCheck(err)
	username, err := flags.GetString("username")
	flagErrCheck(err)
	password, err := flags.GetString("password")
	flagErrCheck(err)
	randomFields, err := flags.GetString("random_fields")
	flagErrCheck(err)

	appContext := producer.AppContext{
		Brokers:  brokers,
		Topic:    topic,
		File:     file,
		Format:   format,
		//Threads:  threads,
		//Duration: duration,
		Username: username,
		Password: password,
	}

	appContext.SetRandomFields(randomFields)
	return appContext
}

func Execute() {
	rootCmd.Flags().StringP("servers", "s","localhost:9092", "kafka brokers location")
	rootCmd.Flags().StringP("topic", "t", "", "topic name")
	rootCmd.Flags().StringP("file", "f", "", "file location")
	rootCmd.Flags().String("format", "json", "format of the file content. Currently only support json")
	//rootCmd.Flags().Int64P("threads", "n", 100, "number of thread will dispatched")
	//rootCmd.Flags().DurationP("duration", "d", time.Minute, "duration of the test")
	rootCmd.Flags().StringP("username", "u", "", "sasl username")
	rootCmd.Flags().StringP("password", "p", "", "sasl password")
	rootCmd.Flags().StringP("random_fields", "r", "", "inject random field if any. Format: field_name:type[,...]. Supported types: uuid, number ")

	rootCmd.MarkFlagRequired("topic")
	rootCmd.MarkFlagRequired("file")

	if err := rootCmd.Execute(); err != nil {
		log.Println(err)
		os.Exit(1)
	}
}

