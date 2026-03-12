// Bazel invoked with --build_event_binary_file outputs a series of delimited build event stream protobuf messages to a file.
// This binary parse that file and output the data to Honeycomb.
// Example:
//   bazel run //bazel/exporter:exporter -- -f (git rev-parse --show-toplevel)/bazel/exporter/testdata/flaky-bep.pb
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/dfinity/ic/proto/build_event_stream"
	"github.com/golang/protobuf/proto"
	beeline "github.com/honeycombio/beeline-go"
	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/encoding/protojson"
)

var GRPC_DIAL_TIMEOUT = 20 * time.Second

// Expect multiple proto messages in uvarint delimited format.
func ReadDelimitedProtoMessage(br *bufio.Reader) ([]byte, error) {
	size, err := binary.ReadUvarint(br)
	if err != nil {
		return nil, err
	}

	msg := make([]byte, size)
	if _, err := io.ReadFull(br, msg); err != nil {
		return nil, fmt.Errorf("error reading protobuf", err)
	}

	return msg, nil
}

func loadEnvVars() map[string]interface{} {
	want := [...]{string{"CD_ENV",
		"CI_COMMIT_AUTHOR",
		"CI_COMMIT_SHA",
		"CI_COMMIT_TAG",
		"CI_COMMIT_TIMESTAMP",
		"CI_CONCURRENT_ID",
		"CI_CONCURRENT_PROJECT_ID",
		"CI_ENVIRONMENT_NAME",
		"CI_ENVIRONMENT_SLUG",
		"CI_EXTERNAL_PULL_REQUEST_IID",
		"CI_EXTERNAL_PULL_REQUEST_SOURCE_BRANCH_NAME",
		"CI_EXTERNAL_PULL_REQUEST_SOURCE_BRANCH_SHA",
		"CI_JOB_ID",
		"CI_JOB_IMAGE",
		"CI_JOB_MANUAL",
		"CI_JOB_NAME",
		"CI_JOB_STAGE",
		"CI_JOB_STATUS",
		"CI_JOB_URL",
		"CI_NODE_INDEX",
		"CI_NODE_TOTAL",
		"CI_PIPELINE_ID",
		"CI_PIPELINE_SOURCE",
		"CI_RUNNER_DESCRIPTION",
		"CI_RUNNER_ID",
		"CI_RUNNER_TAGS",
		"DEPLOY_FLAVOR",
		"USER_ID",
		"USER_LOGIN",
		"SCHEDULE_NAME",
		"TESTNET",
		"STEP_START",
		"PIPELINE_START_TIME",
		"job_status",
		"DISKIMG_BRANCH",
		"CI_MERGE_REQUEST_APPROVED",
		"CI_MERGE_REQUEST_ASSIGNEES",
		"CI_MERGE_REQUEST_ID",
		"CI_MERGE_REQUEST_IID",
		"CI_MERGE_REQUEST_LABELS",
		"CI_MERGE_REQUEST_MILESTONE",
		"CI_MERGE_REQUEST_PROJECT_ID",
		"CI_MERGE_REQUEST_PROJECT_PATH",
		"CI_MERGE_REQUEST_PROJECT_URL",
		"CI_MERGE_REQUEST_REF_PATH",
		"CI_MERGE_REQUEST_SOURCE_BRANCH_NAME",
		"CI_MERGE_REQUEST_SOURCE_BRANCH_SHA",
		"CI_MERGE_REQUEST_SOURCE_PROJECT_ID",
		"CI_MERGE_REQUEST_SOURCE_PROJECT_PATH",
		"CI_MERGE_REQUEST_SOURCE_PROJECT_URL",
		"CI_MERGE_REQUEST_TARGET_BRANCH_NAME",
		"CI_MERGE_REQUEST_TARGET_BRANCH_SHA",
		"CI_MERGE_REQUEST_TITLE",
		"CI_MERGE_REQUEST_EVENT_TYPE",
		"CI_MERGE_REQUEST_DIFF_ID",
		"CI_MERGE_REQUEST_DIFF_BASE_SHA",
	}

	env_vars := make(map[string]interface{})
	for _, w := range want {
		env_vars[w] = os.Getenv(w)
	}
	return env_vars
}

func envVarOrDie(name string) string {
	ans := os.Getenv(name)
	if ans == "" {
		log.Fatalln("Could not load required secret from environment")
	}
	return ans
}

func main() {
	filename := flag.String("f", "", "Bazel build events log protobuff file")
	debug := flag.Bool("n", false, "Debug mode: Output all the proto in text json text form")
	flag.Parse()

	honeycombToken := envVarOrDie("HONEYCOMB_API_TOKEN")
	beeline.Init(beeline.Config{
		WriteKey:    honeycombToken,
		Dataset:     "bazel",
		ServiceName: "exporter",
	})
	defer beeline.Close()

	pbfile, err := os.Open(*filename)
	if err != nil {
		log.Fatalln(err)
	}
	br := bufio.NewReader(pbfile)

	envVars := loadEnvVars()
	log.Println("Reading file", pbfile.Name())
	cnt := 0
	for ; ; cnt++ {
		msg, err := ReadDelimitedProtoMessage(br)
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatalln("failed to read next proto message", err)
		}

		event := &build_event_stream.BuildEvent{}
		if err := proto.Unmarshal(msg, event); err != nil {
			log.Fatalln("Failed to unmarshal", err)
		}
		if *debug {
			fmt.Println(protojson.Format(event))
		}

		// Proto message is oneof many types. Check if it's a test summary message. Otherwise skip it.
		summary := event.GetTestSummary()
		if summary == nil {
			continue
		}

		// Marhsal the protobuf to Json format. This does things like converts proto enums to their string representation.
		b, err := protojson.Marshal(event)
		if err != nil {
			log.Fatalln("failed to marshal protobuf to json:", err)
		}

		jsonMap := make(map[string]interface{})
		if err := json.Unmarshal(b, &jsonMap); err != nil {
			log.Fatalln("failed to unmarshal json bytes to map: ", err)
		}

		spanCtx, eventSpan := beeline.StartSpan(context.Background(), "export_event")
		testTarget := event.GetId().GetTestSummary().GetLabel()
		if IsSystemTestTarget(testTarget) {
			kibanaUrls, failureMessages := ExtractFailuresAndKibanaUrls(testTarget, summary)
			// By adding a string field explicitly, we circumvent the default json encoding behavior of objects (like map) containing special symbols like "&".
			// By default "problematic" HTML characters should be escaped inside JSON quoted strings. https://cs.opensource.google/go/go/+/refs/tags/go1.20.5:src/encoding/json/stream.go;l=193
			// Kibana urls do contain such special symbols (like &) and we don't want to escape them.
			beeline.AddField(spanCtx, "event.kibana_urls", kibanaUrls)
			beeline.AddField(spanCtx, "event.failure_messages", failureMessages)
		}

		beeline.AddField(spanCtx, "event", jsonMap)
		beeline.AddField(spanCtx, "gitlab", envVars)
		eventSpan.Send()
	}
	log.Printf("Processed %d protos", cnt)
}

func ProcessTestLogFile(shouldExtractFailures bool, fileIdx int, file *build_event_stream.File, failuresMap map[string]string, kibanaUrlsMap map[string]string) {
	testLog, err := GetTestLog(file)
	if err != nil {
		errMsg := "Failed to read test log"
		kibanaUrlsMap["url_"+strconv.Itoa(fileIdx)] = errMsg
		if shouldExtractFailures {
			failuresMap["failure_"+strconv.Itoa(fileIdx)] = errMsg
		}
	} else {
		testLogStr := string(testLog)
		kibanaUrl, err := ExtractKibanaUrlFromTestLog(testLogStr)
		if