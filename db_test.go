package fsdb

import(
	"context"
	"testing"
	"os"

	"github.com/tadhunt/logger"
)

// TestCreateDatabase exercises the Firestore admin SDK (databases.create RPC),
// which the local Firestore emulator does NOT implement — every database it
// serves is a single in-memory one. So this test requires a real GCP project
// and is skipped under the emulator-driven `make test` path. To run it against
// a live test project, set FSDB_TEST_PROJECT / FSDB_TEST_DB /
// FSDB_TEST_CREDENTIALS_FILE / FSDB_TEST_ACCESS_TOKEN_FILE and invoke
// `go test -run TestCreateDatabase` directly (no emulator wrapper).
func TestCreateDatabase(t *testing.T) {
	// The emulator never implements databases.create — there's only ever one
	// (in-memory) db. So even if the live-project env vars happen to be set
	// in the developer's shell, skip when we're being driven by the emulator
	// wrapper (`make test`).
	if os.Getenv("FIRESTORE_EMULATOR_HOST") != "" {
		t.Skip("FIRESTORE_EMULATOR_HOST is set; CreateDatabase needs a real GCP project, not the emulator")
	}

	project := os.Getenv("FSDB_TEST_PROJECT")
	db := os.Getenv("FSDB_TEST_DB")
	credentialsFile := os.Getenv("FSDB_TEST_CREDENTIALS_FILE")
	tokenFile := os.Getenv("FSDB_TEST_ACCESS_TOKEN_FILE")

	if project == "" || db == "" || credentialsFile == "" || tokenFile == "" {
		t.Skip("CreateDatabase test requires a live GCP project; set FSDB_TEST_PROJECT/FSDB_TEST_DB/FSDB_TEST_CREDENTIALS_FILE/FSDB_TEST_ACCESS_TOKEN_FILE to run")
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
