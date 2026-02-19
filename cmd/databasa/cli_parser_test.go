package main

import "testing"

func TestParseCLICommandCreateUserAdmin(t *testing.T) {
	cmd, err := parseCLICommand("CREATE USER alice PASSWORD 's3cret' ADMIN;")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	create, ok := cmd.(cliCmdCreateUser)
	if !ok {
		t.Fatalf("unexpected command type %T", cmd)
	}
	if create.Name != "alice" {
		t.Fatalf("unexpected user: %s", create.Name)
	}
	if create.Password != "s3cret" {
		t.Fatalf("unexpected password payload: %s", create.Password)
	}
	if create.PromptPassword {
		t.Fatalf("expected prompt_password=false when literal password is provided")
	}
	if !create.Admin {
		t.Fatalf("expected admin=true")
	}
}

func TestParseCLICommandCreateUserPromptPassword(t *testing.T) {
	cmd, err := parseCLICommand("CREATE USER alice PASSWORD ADMIN;")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	create, ok := cmd.(cliCmdCreateUser)
	if !ok {
		t.Fatalf("unexpected command type %T", cmd)
	}
	if create.Name != "alice" {
		t.Fatalf("unexpected user: %s", create.Name)
	}
	if create.Password != "" {
		t.Fatalf("expected empty literal password when prompt mode is used")
	}
	if !create.PromptPassword {
		t.Fatalf("expected prompt_password=true when no literal password is provided")
	}
	if !create.Admin {
		t.Fatalf("expected admin=true")
	}
}

func TestParseCLICommandAlterUserPromptPassword(t *testing.T) {
	cmd, err := parseCLICommand("ALTER USER alice PASSWORD;")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	alter, ok := cmd.(cliCmdAlterUserPassword)
	if !ok {
		t.Fatalf("unexpected command type %T", cmd)
	}
	if alter.Name != "alice" {
		t.Fatalf("unexpected user: %s", alter.Name)
	}
	if alter.Password != "" {
		t.Fatalf("expected empty literal password when prompt mode is used")
	}
	if !alter.PromptPassword {
		t.Fatalf("expected prompt_password=true when no literal password is provided")
	}
}

func TestParseCLICommandAlterUserLiteralPassword(t *testing.T) {
	cmd, err := parseCLICommand("ALTER USER alice PASSWORD 'n3w';")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	alter, ok := cmd.(cliCmdAlterUserPassword)
	if !ok {
		t.Fatalf("unexpected command type %T", cmd)
	}
	if alter.Name != "alice" {
		t.Fatalf("unexpected user: %s", alter.Name)
	}
	if alter.Password != "n3w" {
		t.Fatalf("unexpected password payload: %s", alter.Password)
	}
	if alter.PromptPassword {
		t.Fatalf("expected prompt_password=false when literal password is provided")
	}
}

func TestParseCLICommandInsertVector(t *testing.T) {
	cmd, err := parseCLICommand("INSERT KEY 'k1' EMBEDDING [1, -2.5, 3e-2];")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	insert, ok := cmd.(cliCmdInsert)
	if !ok {
		t.Fatalf("unexpected command type %T", cmd)
	}
	if insert.Key != "k1" {
		t.Fatalf("unexpected key: %s", insert.Key)
	}
	if len(insert.Embedding) != 3 {
		t.Fatalf("unexpected embedding length: %d", len(insert.Embedding))
	}
}

func TestParseCLICommandBatchInsertFile(t *testing.T) {
	cmd, err := parseCLICommand("BATCH INSERT FILE 'data.jsonl';")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	batch, ok := cmd.(cliCmdBatchInsert)
	if !ok {
		t.Fatalf("unexpected command type %T", cmd)
	}
	if batch.Source != cliBatchSourceFile {
		t.Fatalf("unexpected source: %d", batch.Source)
	}
	if batch.Path != "data.jsonl" {
		t.Fatalf("unexpected path: %s", batch.Path)
	}
}

func TestParseCLICommandSearch(t *testing.T) {
	cmd, err := parseCLICommand("SEARCH TOPK 10 EMBEDDING [0.1, 0.2, 0.3];")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	search, ok := cmd.(cliCmdSearch)
	if !ok {
		t.Fatalf("unexpected command type %T", cmd)
	}
	if search.TopK != 10 {
		t.Fatalf("unexpected topk: %d", search.TopK)
	}
}

func TestParseCLICommandRejectInvalidMetric(t *testing.T) {
	if _, err := parseCLICommand("CREATE COLLECTION feed DIM 3 METRIC manhattan;"); err == nil {
		t.Fatalf("expected parse error for unsupported metric")
	}
}

func TestCLIStatementCompleteIgnoresSemicolonInsideString(t *testing.T) {
	complete, idx, err := cliStatementComplete("INSERT KEY 'a;b' EMBEDDING [1,2];")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !complete {
		t.Fatalf("expected complete statement")
	}
	if idx <= 0 {
		t.Fatalf("expected valid terminator index")
	}
}

func TestCLIStatementCompleteMultilineVector(t *testing.T) {
	stmt := "SEARCH TOPK 3 EMBEDDING [\n1,\n2,\n3\n];"
	complete, _, err := cliStatementComplete(stmt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !complete {
		t.Fatalf("expected complete statement")
	}
}

func TestCLIStatementCompleteRejectsClosingBracketWithoutOpen(t *testing.T) {
	_, _, err := cliStatementComplete("SEARCH TOPK 2 EMBEDDING ];")
	if err == nil {
		t.Fatalf("expected bracket error")
	}
}
