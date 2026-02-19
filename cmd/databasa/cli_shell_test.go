package main

import (
	"reflect"
	"testing"
)

func TestNormalizeCLITLSArgsBareFlag(t *testing.T) {
	got := normalizeCLITLSArgs([]string{"--addr", "127.0.0.1:50051", "--tls", "--insecure"})
	want := []string{"--addr", "127.0.0.1:50051", "--tls=on", "--insecure"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected normalization: got=%v want=%v", got, want)
	}
}

func TestNormalizeCLITLSArgsExplicitValue(t *testing.T) {
	got := normalizeCLITLSArgs([]string{"--tls", "off"})
	want := []string{"--tls=off"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected normalization: got=%v want=%v", got, want)
	}
}

func TestParseCLITLSMode(t *testing.T) {
	on, err := parseCLITLSMode("on")
	if err != nil || !on {
		t.Fatalf("expected on=true, got on=%v err=%v", on, err)
	}
	off, err := parseCLITLSMode("off")
	if err != nil || off {
		t.Fatalf("expected off=false, got off=%v err=%v", off, err)
	}
}

func TestAppendHistorySkipsPasswordStatements(t *testing.T) {
	session := &cliSession{}
	session.appendHistory("CREATE USER alice PASSWORD 'secret' ADMIN;")
	if len(session.history) != 0 {
		t.Fatalf("expected password statement to be excluded from history")
	}

	session.appendHistory("LIST USERS;")
	if len(session.history) != 1 {
		t.Fatalf("expected non-sensitive statement in history")
	}
}

func TestCLIStatementContainsPasswordCaseInsensitive(t *testing.T) {
	if !cliStatementContainsPassword("alter user alice password;") {
		t.Fatalf("expected detector to match PASSWORD case-insensitively")
	}
	if cliStatementContainsPassword("LIST USERS;") {
		t.Fatalf("expected detector to ignore statements without PASSWORD")
	}
}
