package fsdb

import(
	"context"
	"testing"
	"os"

	"github.com/tadhunt/logger"
)

func TestCreateDatabase(t *testing.T) {
	project := os.Getenv("FSDB_TEST_PROJECT")
	db := os.Getenv("FSDB_TEST_DB")
	credentialsFile := os.Getenv("FSDB_TEST_CREDENTIALS_FILE")
	tokenFile := os.Getenv("FSDB_TEST_ACCESS_TOKEN_FILE")

	if project == "" {
		t.Fatalf("FSDB_TEST_PROJECT unset")
	}

	if db == "" {
		t.Fatalf("FSDB_TEST_DB unset")
	}

	if credentialsFile == "" {
		t.Fatalf("FSDB_TEST_CREDENTIALS_FILE unset")
	}

	if tokenFile == "" {
		t.Fatalf("FSDB_TEST_ACCESS_TOKEN_FILE unset")
	}

	credentials := &Credentials{
		File: &credentialsFile,
		AccessTokenFile: &tokenFile,
	}

	ctx := context.Background()
	log := logger.NewTestCompatLogWriter(t)

	output, err := CreateDatabase(ctx, log, project, db, credentials)
	for _, line := range output {
		t.Logf("%s", line)
	}

	if err != nil {
		t.Fatalf("%v", err)
	}
}
