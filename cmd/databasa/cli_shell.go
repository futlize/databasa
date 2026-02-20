package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/futlize/databasa/internal/adminapi"
	pb "github.com/futlize/databasa/pkg/pb"
	"golang.org/x/term"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	structpb "google.golang.org/protobuf/types/known/structpb"
)

type cliOutputFormat string

const (
	cliFormatTable cliOutputFormat = "table"
	cliFormatJSON  cliOutputFormat = "json"
	cliFormatCSV   cliOutputFormat = "csv"
)

type cliConnectionOptions struct {
	addr     string
	timeout  time.Duration
	tls      bool
	caFile   string
	certFile string
	keyFile  string
	insecure bool
	config   string
}

type cliSession struct {
	connOpts     cliConnectionOptions
	authRequired bool

	in  *bufio.Reader
	out io.Writer
	err io.Writer

	conn   *grpc.ClientConn
	client pb.DatabasaClient

	currentCollection string
	format            cliOutputFormat
	timing            bool

	loggedUser  string
	loggedAdmin bool
	authToken   string

	historyPath string
	history     []string
}

func runCLI(args []string) error {
	flagSet := flag.NewFlagSet("databasa cli", flag.ContinueOnError)
	flagSet.SetOutput(os.Stdout)

	configPath := flagSet.String("config", resolvedDefaultConfigPath(), "path to databasa.toml")
	addr := flagSet.String("addr", "", "loopback gRPC address (default 127.0.0.1:<port> from config)")
	timeout := flagSet.Duration("timeout", 5*time.Second, "RPC/connect timeout")
	tlsMode := flagSet.String("tls", "", "transport security: on|off (default from config)")
	caFile := flagSet.String("ca", "", "CA bundle file for TLS server verification")
	certFile := flagSet.String("cert", "", "client certificate file (mTLS)")
	keyFile := flagSet.String("key", "", "client private key file (mTLS)")
	insecureMode := flagSet.Bool("insecure", false, "skip TLS certificate verification (development only)")

	normalizedArgs := normalizeCLITLSArgs(args)
	if err := flagSet.Parse(normalizedArgs); err != nil {
		return err
	}
	if flagSet.NArg() > 0 {
		return fmt.Errorf("unexpected trailing arguments: %s", strings.Join(flagSet.Args(), " "))
	}

	cfg, _, err := LoadOrCreateConfig(*configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	targetAddr := strings.TrimSpace(*addr)
	if targetAddr == "" {
		targetAddr = fmt.Sprintf("127.0.0.1:%d", cfg.Server.Port)
	}
	if err := validateCLILocalAddress(targetAddr); err != nil {
		return err
	}

	tlsEnabled := cfg.Security.TLSEnabled
	if strings.TrimSpace(*tlsMode) != "" {
		parsed, parseErr := parseCLITLSMode(*tlsMode)
		if parseErr != nil {
			return parseErr
		}
		tlsEnabled = parsed
	}

	connOpts := cliConnectionOptions{
		addr:     targetAddr,
		timeout:  *timeout,
		tls:      tlsEnabled,
		caFile:   strings.TrimSpace(*caFile),
		certFile: strings.TrimSpace(*certFile),
		keyFile:  strings.TrimSpace(*keyFile),
		insecure: *insecureMode,
		config:   *configPath,
	}

	historyPath, historyErr := resolveCLIHistoryPath()
	if historyErr != nil {
		return historyErr
	}

	session := &cliSession{
		connOpts:     connOpts,
		authRequired: cfg.Security.AuthEnabled && cfg.Security.RequireAuth,
		in:           bufio.NewReader(os.Stdin),
		out:          os.Stdout,
		err:          os.Stderr,
		format:       cliFormatTable,
		historyPath:  historyPath,
	}
	session.loadHistory()

	if err := session.connect(); err != nil {
		return fmt.Errorf("connect %s: %w", session.connOpts.addr, err)
	}
	if err := session.prepareSessionAuth(); err != nil {
		return err
	}

	fmt.Fprintln(session.out, "Databasa interactive CLI. Type \\help for help.")
	defer func() {
		session.saveHistory()
		if session.conn != nil {
			_ = session.conn.Close()
		}
	}()

	for {
		stmt, readErr := session.readStatement()
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				fmt.Fprintln(session.out)
				return nil
			}
			fmt.Fprintf(session.err, "error: %s\n", formatCLIError(readErr))
			continue
		}
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		session.appendHistory(stmt)

		start := time.Now()
		if strings.HasPrefix(strings.TrimSpace(stmt), "\\") {
			quit, metaErr := session.executeMeta(strings.TrimSpace(stmt))
			if metaErr != nil {
				fmt.Fprintf(session.err, "error: %s\n", formatCLIError(metaErr))
			}
			if session.timing {
				fmt.Fprintf(session.out, "time: %s\n", time.Since(start).Truncate(time.Microsecond))
			}
			if quit {
				return nil
			}
			continue
		}

		cmd, parseErr := parseCLICommand(stmt)
		if parseErr != nil {
			fmt.Fprintf(session.err, "parse error: %s\n", formatCLIError(parseErr))
			continue
		}

		if execErr := session.executeCommand(cmd); execErr != nil {
			fmt.Fprintf(session.err, "error: %s\n", formatCLIError(execErr))
		}
		if session.timing {
			fmt.Fprintf(session.out, "time: %s\n", time.Since(start).Truncate(time.Microsecond))
		}
	}
}

func parseCLITLSMode(raw string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "on", "true", "1", "yes":
		return true, nil
	case "off", "false", "0", "no":
		return false, nil
	default:
		return false, fmt.Errorf("invalid --tls value %q (expected on|off)", raw)
	}
}

func validateCLILocalAddress(addr string) error {
	trimmed := strings.TrimSpace(addr)
	if trimmed == "" {
		return errors.New("cli requires a local loopback address")
	}
	host, _, err := net.SplitHostPort(trimmed)
	if err != nil {
		return fmt.Errorf("invalid --addr %q (expected host:port): %w", addr, err)
	}
	host = strings.TrimSpace(host)
	if strings.EqualFold(host, "localhost") {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("cli local-only mode: %q is not loopback; use 127.0.0.1:<port> or localhost:<port>", addr)
	}
	return nil
}

func normalizeCLITLSArgs(args []string) []string {
	out := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		current := args[i]
		if current != "--tls" {
			out = append(out, current)
			continue
		}

		nextIsValue := i+1 < len(args) && !strings.HasPrefix(args[i+1], "-")
		if nextIsValue {
			out = append(out, "--tls="+args[i+1])
			i++
			continue
		}
		out = append(out, "--tls=on")
	}
	return out
}

func (s *cliSession) prepareSessionAuth() error {
	if !s.authRequired {
		return nil
	}
	if strings.TrimSpace(s.loggedUser) != "" && strings.TrimSpace(s.authToken) != "" {
		return nil
	}

	hasUsers, err := s.bootstrapHasUsers()
	if err != nil {
		return err
	}
	if !hasUsers {
		return s.bootstrapInitialAdmin()
	}

	const maxAttempts = 3
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		userName, userErr := promptLine(s.in, s.out, "user: ")
		if userErr != nil {
			return userErr
		}
		secret, secretErr := promptHidden("password or api key: ")
		if secretErr != nil {
			return secretErr
		}
		if loginErr := s.loginWithSecret(strings.TrimSpace(userName), strings.TrimSpace(secret), false); loginErr != nil {
			fmt.Fprintf(s.err, "authentication failed (%d/%d): %s\n", attempt, maxAttempts, formatCLIError(loginErr))
			continue
		}
		fmt.Fprintf(s.out, "authenticated as %s\n", strings.TrimSpace(userName))
		return nil
	}
	return errors.New("authentication failed after maximum attempts")
}

func (s *cliSession) bootstrapInitialAdmin() error {
	fmt.Fprintln(s.out, "No users found. Bootstrap initial admin user.")
	userName, err := promptLine(s.in, s.out, "admin user: ")
	if err != nil {
		return err
	}
	password, err := promptHidden("admin password: ")
	if err != nil {
		return err
	}
	confirm, err := promptHidden("confirm password: ")
	if err != nil {
		return err
	}
	if password != confirm {
		return errors.New("password confirmation does not match")
	}
	userName = strings.TrimSpace(userName)
	password = strings.TrimSpace(password)
	if userName == "" {
		return errors.New("admin user cannot be empty")
	}
	if password == "" {
		return errors.New("admin password cannot be empty")
	}

	created, err := s.adminCreateUser(userName, password, true)
	if err != nil {
		return fmt.Errorf("bootstrap create admin user: %w", err)
	}
	if err := s.loginWithSecret(userName, password, false); err != nil {
		return fmt.Errorf("bootstrap login failed: %w", err)
	}
	fmt.Fprintf(s.out, "bootstrap admin %s created and authenticated\n", userName)
	fmt.Fprintf(s.out, "key id: %s\n", created.keyID)
	fmt.Fprintf(s.out, "api key (shown once): %s\n", created.apiKey)
	fmt.Fprintln(s.out, "store this api key securely. Databasa never stores plaintext keys.")
	return nil
}

func promptLine(reader *bufio.Reader, w io.Writer, label string) (string, error) {
	fmt.Fprint(w, label)
	line, err := reader.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return "", err
	}
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		if errors.Is(err, io.EOF) {
			return "", io.EOF
		}
		return "", errors.New("value cannot be empty")
	}
	return trimmed, nil
}

func resolveCLIHistoryPath() (string, error) {
	base, err := os.UserConfigDir()
	if err != nil {
		home, homeErr := os.UserHomeDir()
		if homeErr != nil {
			return "", fmt.Errorf("resolve history path: %w", err)
		}
		base = filepath.Join(home, ".config")
	}
	dir := filepath.Join(base, "databasa")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("create history dir %q: %w", dir, err)
	}
	return filepath.Join(dir, "cli_history"), nil
}

func (s *cliSession) loadHistory() {
	data, err := os.ReadFile(s.historyPath)
	if err != nil {
		return
	}
	lines := strings.Split(string(data), "\n")
	s.history = s.history[:0]
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		s.history = append(s.history, trimmed)
	}
}

func (s *cliSession) saveHistory() {
	if s.historyPath == "" || len(s.history) == 0 {
		return
	}
	maxEntries := 2000
	start := 0
	if len(s.history) > maxEntries {
		start = len(s.history) - maxEntries
	}
	body := strings.Join(s.history[start:], "\n") + "\n"
	_ = os.WriteFile(s.historyPath, []byte(body), 0o600)
}

func (s *cliSession) appendHistory(statement string) {
	trimmed := strings.TrimSpace(statement)
	if trimmed == "" {
		return
	}
	if cliStatementContainsPassword(trimmed) {
		return
	}
	s.history = append(s.history, trimmed)
}

func cliStatementContainsPassword(statement string) bool {
	return strings.Contains(strings.ToUpper(statement), "PASSWORD")
}

func (s *cliSession) prompt() string {
	addr := s.connOpts.addr
	if strings.TrimSpace(addr) == "" {
		addr = "disconnected"
	}
	collection := strings.TrimSpace(s.currentCollection)
	if collection == "" {
		collection = "-"
	}
	return fmt.Sprintf("databasa(%s/%s)> ", addr, collection)
}

func (s *cliSession) readStatement() (string, error) {
	var buf strings.Builder
	for {
		prompt := s.prompt()
		if buf.Len() > 0 {
			prompt = "... "
		}
		fmt.Fprint(s.out, prompt)

		line, err := s.in.ReadString('\n')
		if err != nil && !errors.Is(err, io.EOF) {
			return "", err
		}

		if buf.Len() == 0 {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "\\") {
				return trimmed, nil
			}
			if trimmed == "" && errors.Is(err, io.EOF) {
				return "", io.EOF
			}
		}

		buf.WriteString(line)
		raw := buf.String()
		complete, idx, compErr := cliStatementComplete(raw)
		if compErr != nil {
			return "", compErr
		}
		if complete {
			stmt := strings.TrimSpace(raw[:idx])
			rest := strings.TrimSpace(raw[idx+1:])
			if rest != "" {
				return "", errors.New("multiple statements are not supported; run one statement per terminator")
			}
			return stmt, nil
		}
		if errors.Is(err, io.EOF) {
			if strings.TrimSpace(raw) == "" {
				return "", io.EOF
			}
			return strings.TrimSpace(raw), nil
		}
	}
}

func (s *cliSession) executeMeta(raw string) (bool, error) {
	fields := strings.Fields(raw)
	if len(fields) == 0 {
		return false, nil
	}

	switch fields[0] {
	case "\\q", "\\quit":
		return true, nil
	case "\\help":
		s.printHelp()
		return false, nil
	case "\\connect":
		if len(fields) != 2 {
			return false, errors.New("usage: \\connect <addr> (loopback only)")
		}
		target := strings.TrimSpace(fields[1])
		if err := validateCLILocalAddress(target); err != nil {
			return false, err
		}
		s.connOpts.addr = target
		if err := s.connect(); err != nil {
			return false, err
		}
		if err := s.prepareSessionAuth(); err != nil {
			return false, err
		}
		fmt.Fprintf(s.out, "connected to %s\n", s.connOpts.addr)
		return false, nil
	case "\\use":
		if len(fields) != 2 {
			return false, errors.New("usage: \\use <collection>")
		}
		if err := s.ensureCollectionExists(fields[1]); err != nil {
			return false, err
		}
		s.currentCollection = fields[1]
		return false, nil
	case "\\collections":
		return false, s.executeCommand(cliCmdListCollections{})
	case "\\users":
		return false, s.executeCommand(cliCmdListUsers{})
	case "\\timing":
		if len(fields) != 2 {
			return false, errors.New("usage: \\timing on|off")
		}
		switch strings.ToLower(fields[1]) {
		case "on":
			s.timing = true
		case "off":
			s.timing = false
		default:
			return false, errors.New("usage: \\timing on|off")
		}
		fmt.Fprintf(s.out, "timing is %s\n", map[bool]string{true: "on", false: "off"}[s.timing])
		return false, nil
	case "\\format":
		if len(fields) != 2 {
			return false, errors.New("usage: \\format table|json|csv")
		}
		format := cliOutputFormat(strings.ToLower(strings.TrimSpace(fields[1])))
		switch format {
		case cliFormatTable, cliFormatJSON, cliFormatCSV:
		default:
			return false, errors.New("usage: \\format table|json|csv")
		}
		s.format = format
		fmt.Fprintf(s.out, "output format: %s\n", s.format)
		return false, nil
	case "\\login":
		if len(fields) != 2 {
			return false, errors.New("usage: \\login <username>")
		}
		return false, s.login(fields[1])
	case "\\logout":
		return false, s.logout()
	case "\\whoami":
		if strings.TrimSpace(s.loggedUser) == "" {
			fmt.Fprintln(s.out, "not logged in")
			return false, nil
		}
		role := "user"
		if s.loggedAdmin {
			role = "admin"
		}
		fmt.Fprintf(s.out, "%s (%s)\n", s.loggedUser, role)
		return false, nil
	default:
		return false, fmt.Errorf("unknown meta-command %q (try \\help)", fields[0])
	}
}

func (s *cliSession) printHelp() {
	fmt.Fprintln(s.out, `Meta-commands:
  \help
  \quit, \q
  \connect <addr> (loopback only)
  \use <collection>
  \collections
  \users
  \login <username>
  \logout
  \whoami
  \timing on|off
  \format table|json|csv

Core commands (terminate with ';'):
  CREATE USER <name> PASSWORD [ADMIN];
  ALTER USER <name> PASSWORD;
  DROP USER <name>;
  LIST USERS;
  (secret is always entered via hidden prompt)

  CREATE COLLECTION <name> DIM <n> [METRIC cosine|dot|l2];
  DROP COLLECTION <name>;
  DESCRIBE COLLECTION <name>;
  LIST COLLECTIONS;
  USE <name>;

  INSERT KEY '<k>' EMBEDDING [1.0, 2.0, ...];
  BATCH INSERT FILE '<path-to-jsonl>';
  BATCH INSERT STDIN;
  SEARCH TOPK <k> EMBEDDING [1.0, 2.0, ...];
  DELETE KEY '<k>';
  GET KEY '<k>';`)
}

func (s *cliSession) connect() error {
	creds, err := s.buildClientCredentials()
	if err != nil {
		return err
	}
	previousToken := s.authToken
	previousUser := s.loggedUser

	ctx, cancel := context.WithTimeout(context.Background(), s.connOpts.timeout)
	defer cancel()

	conn, err := grpc.DialContext(
		ctx,
		s.connOpts.addr,
		grpc.WithBlock(),
		grpc.WithTransportCredentials(creds),
	)
	if err != nil {
		return err
	}

	if s.conn != nil {
		_ = s.conn.Close()
	}
	s.conn = conn
	s.client = pb.NewDatabasaClient(conn)
	s.loggedUser = ""
	s.loggedAdmin = false
	s.authToken = ""
	if strings.TrimSpace(previousToken) != "" && strings.TrimSpace(previousUser) != "" {
		if err := s.loginWithSecret(previousUser, previousToken, false); err != nil {
			s.loggedUser = ""
			s.loggedAdmin = false
			s.authToken = ""
		}
	}
	return nil
}

func (s *cliSession) buildClientCredentials() (credentials.TransportCredentials, error) {
	if !s.connOpts.tls {
		return insecure.NewCredentials(), nil
	}

	tlsConfig := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: s.connOpts.insecure,
	}

	if s.connOpts.caFile != "" {
		data, err := os.ReadFile(s.connOpts.caFile)
		if err != nil {
			return nil, fmt.Errorf("read ca file %q: %w", s.connOpts.caFile, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(data) {
			return nil, fmt.Errorf("parse ca file %q: no certificates found", s.connOpts.caFile)
		}
		tlsConfig.RootCAs = pool
	}

	if s.connOpts.certFile != "" || s.connOpts.keyFile != "" {
		if s.connOpts.certFile == "" || s.connOpts.keyFile == "" {
			return nil, errors.New("mTLS requires both --cert and --key")
		}
		pair, err := tls.LoadX509KeyPair(s.connOpts.certFile, s.connOpts.keyFile)
		if err != nil {
			return nil, fmt.Errorf("load client certificate/key: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{pair}
	}

	return credentials.NewTLS(tlsConfig), nil
}

func (s *cliSession) login(userName string) error {
	if s.client == nil {
		return errors.New("not connected to server")
	}
	userName = strings.TrimSpace(userName)
	if userName == "" {
		return errors.New("usage: \\login <username>")
	}

	secret, err := promptHidden("password or api key: ")
	if err != nil {
		return err
	}
	secret = strings.TrimSpace(secret)
	if secret == "" {
		return errors.New("password cannot be empty")
	}
	return s.loginWithSecret(userName, secret, true)
}

func (s *cliSession) loginWithSecret(userName, secret string, verbose bool) error {
	if strings.HasPrefix(secret, "dbs1.") {
		return s.loginWithToken(userName, secret, verbose)
	}
	return s.loginWithCredentials(userName, secret, verbose)
}

func (s *cliSession) loginWithToken(userName, token string, verbose bool) error {
	return s.loginWithPayload(userName, map[string]any{
		"api_key": token,
	}, verbose)
}

func (s *cliSession) loginWithCredentials(userName, secret string, verbose bool) error {
	return s.loginWithPayload(userName, map[string]any{
		"username": userName,
		"secret":   secret,
	}, verbose)
}

func (s *cliSession) loginWithPayload(userName string, payload map[string]any, verbose bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.connOpts.timeout)
	defer cancel()

	body, err := structpb.NewStruct(payload)
	if err != nil {
		return err
	}
	if err := s.conn.Invoke(ctx, adminapi.FullMethodLogin, body, &emptypb.Empty{}); err != nil {
		return err
	}
	rememberedSecret := ""
	if apiKey, ok := payload["api_key"].(string); ok {
		rememberedSecret = strings.TrimSpace(apiKey)
	} else if secret, ok := payload["secret"].(string); ok {
		rememberedSecret = strings.TrimSpace(secret)
	}
	s.loggedAdmin = false
	if isAdmin, adminErr := s.detectAdminStatus(); adminErr == nil {
		s.loggedAdmin = isAdmin
	}

	s.loggedUser = userName
	s.authToken = rememberedSecret
	if verbose {
		fmt.Fprintf(s.out, "login ok for %s\n", userName)
		if s.loggedAdmin {
			fmt.Fprintln(s.out, "role: admin")
		}
	}
	return nil
}

func (s *cliSession) detectAdminStatus() (bool, error) {
	if s.conn == nil {
		return false, errors.New("not connected to server")
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.connOpts.timeout)
	defer cancel()
	var out structpb.Struct
	err := s.conn.Invoke(ctx, adminapi.FullMethodListUsers, &emptypb.Empty{}, &out)
	if err != nil {
		if status.Code(err) == codes.PermissionDenied || status.Code(err) == codes.Unauthenticated {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *cliSession) logout() error {
	if s.conn == nil {
		s.loggedUser = ""
		s.loggedAdmin = false
		s.authToken = ""
		fmt.Fprintln(s.out, "logged out")
		return nil
	}
	s.loggedUser = ""
	s.loggedAdmin = false
	s.authToken = ""
	if err := s.connect(); err != nil {
		return err
	}
	fmt.Fprintln(s.out, "logged out")
	return nil
}

func promptHidden(prompt string) (string, error) {
	fmt.Fprint(os.Stdout, prompt)
	secret, err := term.ReadPassword(int(os.Stdin.Fd()))
	fmt.Fprintln(os.Stdout)
	if err != nil {
		return "", fmt.Errorf("read hidden input: %w", err)
	}
	return string(secret), nil
}

func (s *cliSession) executeCommand(cmd cliCommand) error {
	switch c := cmd.(type) {
	case cliCmdCreateUser:
		return s.execCreateUser(c)
	case cliCmdAlterUserPassword:
		return s.execAlterUserPassword(c)
	case cliCmdDropUser:
		return s.execDropUser(c)
	case cliCmdListUsers:
		return s.execListUsers()
	case cliCmdCreateCollection:
		return s.execCreateCollection(c)
	case cliCmdDropCollection:
		return s.execDropCollection(c)
	case cliCmdDescribeCollection:
		return s.execDescribeCollection(c)
	case cliCmdListCollections:
		return s.execListCollections()
	case cliCmdUseCollection:
		return s.execUseCollection(c)
	case cliCmdInsert:
		return s.execInsert(c)
	case cliCmdBatchInsert:
		return s.execBatchInsert(c)
	case cliCmdSearch:
		return s.execSearch(c)
	case cliCmdDelete:
		return s.execDelete(c)
	case cliCmdGet:
		return s.execGet(c)
	default:
		return fmt.Errorf("unsupported command type %T", cmd)
	}
}

func (s *cliSession) requireCollection() (string, error) {
	name := strings.TrimSpace(s.currentCollection)
	if name == "" {
		return "", errors.New("no collection selected; run USE <name>; or \\use <name>")
	}
	return name, nil
}

func (s *cliSession) rpcContext() (context.Context, context.CancelFunc) {
	timeout := s.connOpts.timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return context.WithTimeout(context.Background(), timeout)
}

func (s *cliSession) ensureConnected() error {
	if s.client == nil {
		return errors.New("not connected to server")
	}
	return nil
}

type cliUserCreateResult struct {
	name   string
	roles  []string
	keyID  string
	apiKey string
}

type cliUserPasswordResult struct {
	name   string
	keyID  string
	apiKey string
}

type cliUserRecord struct {
	name       string
	roles      []string
	disabled   bool
	activeKeys int
	totalKeys  int
}

func (s *cliSession) bootstrapHasUsers() (bool, error) {
	if err := s.ensureConnected(); err != nil {
		return false, err
	}
	ctx, cancel := s.rpcContext()
	defer cancel()
	var out structpb.Struct
	if err := s.conn.Invoke(ctx, adminapi.FullMethodBootstrapStatus, &emptypb.Empty{}, &out); err != nil {
		return false, err
	}
	value := out.GetFields()["has_users"]
	if value == nil {
		return false, errors.New("invalid bootstrap status response")
	}
	return value.GetBoolValue(), nil
}

func (s *cliSession) adminCreateUser(name, secret string, admin bool) (cliUserCreateResult, error) {
	if err := s.ensureConnected(); err != nil {
		return cliUserCreateResult{}, err
	}
	ctx, cancel := s.rpcContext()
	defer cancel()
	in, err := structpb.NewStruct(map[string]any{
		"name":   name,
		"secret": secret,
		"admin":  admin,
	})
	if err != nil {
		return cliUserCreateResult{}, err
	}
	var out structpb.Struct
	if err := s.conn.Invoke(ctx, adminapi.FullMethodCreateUser, in, &out); err != nil {
		return cliUserCreateResult{}, err
	}
	result := cliUserCreateResult{
		name:   structFieldString(&out, "name"),
		keyID:  structFieldString(&out, "key_id"),
		apiKey: structFieldString(&out, "api_key"),
	}
	result.roles = structStringList(&out, "roles")
	return result, nil
}

func (s *cliSession) adminAlterUserPassword(name, secret string) (cliUserPasswordResult, error) {
	if err := s.ensureConnected(); err != nil {
		return cliUserPasswordResult{}, err
	}
	ctx, cancel := s.rpcContext()
	defer cancel()
	in, err := structpb.NewStruct(map[string]any{
		"name":   name,
		"secret": secret,
	})
	if err != nil {
		return cliUserPasswordResult{}, err
	}
	var out structpb.Struct
	if err := s.conn.Invoke(ctx, adminapi.FullMethodAlterUserPassword, in, &out); err != nil {
		return cliUserPasswordResult{}, err
	}
	return cliUserPasswordResult{
		name:   structFieldString(&out, "name"),
		keyID:  structFieldString(&out, "key_id"),
		apiKey: structFieldString(&out, "api_key"),
	}, nil
}

func (s *cliSession) adminDropUser(name string) error {
	if err := s.ensureConnected(); err != nil {
		return err
	}
	ctx, cancel := s.rpcContext()
	defer cancel()
	in, err := structpb.NewStruct(map[string]any{"name": name})
	if err != nil {
		return err
	}
	return s.conn.Invoke(ctx, adminapi.FullMethodDropUser, in, &emptypb.Empty{})
}

func (s *cliSession) adminListUsers() ([]cliUserRecord, error) {
	if err := s.ensureConnected(); err != nil {
		return nil, err
	}
	ctx, cancel := s.rpcContext()
	defer cancel()
	var out structpb.Struct
	if err := s.conn.Invoke(ctx, adminapi.FullMethodListUsers, &emptypb.Empty{}, &out); err != nil {
		return nil, err
	}
	usersValue := out.GetFields()["users"]
	if usersValue == nil {
		return nil, nil
	}
	list := usersValue.GetListValue()
	if list == nil {
		return nil, errors.New("invalid users payload")
	}
	records := make([]cliUserRecord, 0, len(list.Values))
	for _, item := range list.Values {
		userStruct := item.GetStructValue()
		if userStruct == nil {
			continue
		}
		records = append(records, cliUserRecord{
			name:       structFieldString(userStruct, "name"),
			roles:      structStringList(userStruct, "roles"),
			disabled:   structFieldBool(userStruct, "disabled"),
			activeKeys: int(structFieldNumber(userStruct, "active_keys")),
			totalKeys:  int(structFieldNumber(userStruct, "total_keys")),
		})
	}
	return records, nil
}

func structStringList(payload interface {
	GetFields() map[string]*structpb.Value
}, field string) []string {
	values := payload.GetFields()[field]
	if values == nil || values.GetListValue() == nil {
		return nil
	}
	out := make([]string, 0, len(values.GetListValue().Values))
	for _, item := range values.GetListValue().Values {
		value := strings.TrimSpace(item.GetStringValue())
		if value == "" {
			continue
		}
		out = append(out, value)
	}
	return out
}

func structFieldString(payload interface {
	GetFields() map[string]*structpb.Value
}, field string) string {
	value := payload.GetFields()[field]
	if value == nil {
		return ""
	}
	return strings.TrimSpace(value.GetStringValue())
}

func structFieldBool(payload interface {
	GetFields() map[string]*structpb.Value
}, field string) bool {
	value := payload.GetFields()[field]
	if value == nil {
		return false
	}
	return value.GetBoolValue()
}

func structFieldNumber(payload interface {
	GetFields() map[string]*structpb.Value
}, field string) float64 {
	value := payload.GetFields()[field]
	if value == nil {
		return 0
	}
	return value.GetNumberValue()
}
