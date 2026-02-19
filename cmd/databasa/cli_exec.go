package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/futlize/databasa/internal/security"
	pb "github.com/futlize/databasa/pkg/pb"
)

func (s *cliSession) execCreateUser(cmd cliCmdCreateUser) error {
	if err := s.requireAdmin(); err != nil {
		bootstrap, bootstrapErr := s.isBootstrapUserCreation()
		if bootstrapErr != nil {
			return err
		}
		if !bootstrap {
			return err
		}
		if !cmd.Admin {
			return errors.New("first user bootstrap must include ADMIN role")
		}
		fmt.Fprintln(s.out, "bootstrap mode: creating first admin user without prior login")
	}
	secret, err := s.resolveUserSecret(cmd.Password, cmd.PromptPassword)
	if err != nil {
		return err
	}
	roles := []security.Role{security.RoleRead}
	if cmd.Admin {
		roles = []security.Role{security.RoleAdmin}
	}
	user, generated, err := s.manager.CreateUserWithPassword(cmd.Name, roles, "password", secret)
	if err != nil {
		return err
	}
	fmt.Fprintf(s.out, "created user: %s (roles=%s)\n", user.Name, rolesToText(user.Roles))
	fmt.Fprintf(s.out, "key id: %s\n", generated.KeyID)
	fmt.Fprintf(s.out, "api key (shown once): %s\n", generated.Plaintext)
	fmt.Fprintln(s.out, "store this api key securely. Databasa never stores plaintext keys.")
	return nil
}

func (s *cliSession) isBootstrapUserCreation() (bool, error) {
	records, err := s.manager.ListUsers()
	if err != nil {
		return false, err
	}
	return len(records) == 0, nil
}

func (s *cliSession) execAlterUserPassword(cmd cliCmdAlterUserPassword) error {
	if err := s.requireAdmin(); err != nil {
		return err
	}
	secret, err := s.resolveUserSecret(cmd.Password, cmd.PromptPassword)
	if err != nil {
		return err
	}
	generated, err := s.manager.AlterUserPassword(cmd.Name, "password", secret)
	if err != nil {
		return err
	}
	fmt.Fprintf(s.out, "updated password for user %s (new key id=%s)\n", cmd.Name, generated.KeyID)
	fmt.Fprintf(s.out, "api key (shown once): %s\n", generated.Plaintext)
	fmt.Fprintln(s.out, "store this api key securely. Databasa never stores plaintext keys.")
	return nil
}

func (s *cliSession) execDropUser(cmd cliCmdDropUser) error {
	if err := s.requireAdmin(); err != nil {
		return err
	}
	if strings.EqualFold(strings.TrimSpace(cmd.Name), strings.TrimSpace(s.loggedUser)) {
		return errors.New("cannot drop currently authenticated user")
	}
	if err := s.manager.DeleteUser(cmd.Name); err != nil {
		return err
	}
	fmt.Fprintf(s.out, "dropped user %s\n", cmd.Name)
	return nil
}

func (s *cliSession) execListUsers() error {
	if err := s.requireAdmin(); err != nil {
		return err
	}
	records, err := s.manager.ListUsers()
	if err != nil {
		return err
	}

	switch s.format {
	case cliFormatJSON:
		return printJSON(s.out, records)
	case cliFormatCSV:
		rows := make([][]string, 0, len(records)+1)
		rows = append(rows, []string{"name", "roles", "disabled", "active_keys", "total_keys"})
		for _, record := range records {
			rows = append(rows, []string{
				record.Name,
				rolesToText(record.Roles),
				strconv.FormatBool(record.Disabled),
				strconv.Itoa(record.ActiveKeys),
				strconv.Itoa(record.TotalKeys),
			})
		}
		return printCSVRows(s.out, rows)
	default:
		rows := make([][]string, 0, len(records))
		for _, record := range records {
			rows = append(rows, []string{
				record.Name,
				rolesToText(record.Roles),
				strconv.FormatBool(record.Disabled),
				strconv.Itoa(record.ActiveKeys),
				strconv.Itoa(record.TotalKeys),
			})
		}
		printTable(s.out, []string{"name", "roles", "disabled", "active_keys", "total_keys"}, rows)
		return nil
	}
}

func (s *cliSession) execCreateCollection(cmd cliCmdCreateCollection) error {
	if err := s.ensureConnected(); err != nil {
		return err
	}
	metric, err := metricFromText(cmd.Metric)
	if err != nil {
		return err
	}

	ctx, cancel := s.rpcContext()
	defer cancel()
	_, err = s.client.CreateCollection(ctx, &pb.CreateCollectionRequest{
		Name:      cmd.Name,
		Dimension: uint32(cmd.Dim),
		Metric:    metric,
	})
	if err != nil {
		return err
	}
	fmt.Fprintf(s.out, "created collection %s\n", cmd.Name)
	return nil
}

func (s *cliSession) execDropCollection(cmd cliCmdDropCollection) error {
	if err := s.ensureConnected(); err != nil {
		return err
	}
	ctx, cancel := s.rpcContext()
	defer cancel()
	_, err := s.client.DeleteCollection(ctx, &pb.DeleteCollectionRequest{Name: cmd.Name})
	if err != nil {
		return err
	}
	if strings.EqualFold(s.currentCollection, cmd.Name) {
		s.currentCollection = ""
	}
	fmt.Fprintf(s.out, "dropped collection %s\n", cmd.Name)
	return nil
}

func (s *cliSession) execDescribeCollection(cmd cliCmdDescribeCollection) error {
	if err := s.ensureConnected(); err != nil {
		return err
	}
	info, err := s.lookupCollection(cmd.Name)
	if err != nil {
		return err
	}
	if info == nil {
		return fmt.Errorf("not found: collection %s", cmd.Name)
	}

	switch s.format {
	case cliFormatJSON:
		return printJSON(s.out, info)
	case cliFormatCSV:
		rows := [][]string{
			{"name", "dimension", "metric", "count", "has_index"},
			{info.Name, strconv.FormatUint(uint64(info.Dimension), 10), info.Metric.String(), strconv.FormatUint(info.Count, 10), strconv.FormatBool(info.HasIndex)},
		}
		return printCSVRows(s.out, rows)
	default:
		printTable(s.out, []string{"field", "value"}, [][]string{
			{"name", info.Name},
			{"dimension", strconv.FormatUint(uint64(info.Dimension), 10)},
			{"metric", info.Metric.String()},
			{"count", strconv.FormatUint(info.Count, 10)},
			{"has_index", strconv.FormatBool(info.HasIndex)},
		})
		return nil
	}
}

func (s *cliSession) execListCollections() error {
	if err := s.ensureConnected(); err != nil {
		return err
	}

	ctx, cancel := s.rpcContext()
	defer cancel()
	resp, err := s.client.ListCollections(ctx, &pb.ListCollectionsRequest{})
	if err != nil {
		return err
	}

	switch s.format {
	case cliFormatJSON:
		return printJSON(s.out, resp.Collections)
	case cliFormatCSV:
		rows := make([][]string, 0, len(resp.Collections)+1)
		rows = append(rows, []string{"name", "dimension", "metric", "count", "has_index"})
		for _, col := range resp.Collections {
			rows = append(rows, []string{
				col.Name,
				strconv.FormatUint(uint64(col.Dimension), 10),
				col.Metric.String(),
				strconv.FormatUint(col.Count, 10),
				strconv.FormatBool(col.HasIndex),
			})
		}
		return printCSVRows(s.out, rows)
	default:
		rows := make([][]string, 0, len(resp.Collections))
		for _, col := range resp.Collections {
			rows = append(rows, []string{
				col.Name,
				strconv.FormatUint(uint64(col.Dimension), 10),
				col.Metric.String(),
				strconv.FormatUint(col.Count, 10),
				strconv.FormatBool(col.HasIndex),
			})
		}
		printTable(s.out, []string{"name", "dimension", "metric", "count", "has_index"}, rows)
		return nil
	}
}

func (s *cliSession) execUseCollection(cmd cliCmdUseCollection) error {
	if err := s.ensureCollectionExists(cmd.Name); err != nil {
		return err
	}
	s.currentCollection = cmd.Name
	fmt.Fprintf(s.out, "using collection %s\n", cmd.Name)
	return nil
}

func (s *cliSession) execInsert(cmd cliCmdInsert) error {
	if err := s.ensureConnected(); err != nil {
		return err
	}
	collection, err := s.requireCollection()
	if err != nil {
		return err
	}

	ctx, cancel := s.rpcContext()
	defer cancel()
	_, err = s.client.Insert(ctx, &pb.InsertRequest{
		Collection: collection,
		Key:        cmd.Key,
		Embedding:  cmd.Embedding,
	})
	if err != nil {
		return err
	}
	fmt.Fprintln(s.out, "inserted 1 vector")
	return nil
}

func (s *cliSession) execBatchInsert(cmd cliCmdBatchInsert) error {
	if err := s.ensureConnected(); err != nil {
		return err
	}
	collection, err := s.requireCollection()
	if err != nil {
		return err
	}

	items, err := s.loadBatchItems(cmd)
	if err != nil {
		return err
	}
	if len(items) == 0 {
		return errors.New("batch insert has no items")
	}

	var inserted uint64
	const chunkSize = 1000
	for start := 0; start < len(items); start += chunkSize {
		end := start + chunkSize
		if end > len(items) {
			end = len(items)
		}
		ctx, cancel := s.rpcContext()
		resp, callErr := s.client.BatchInsert(ctx, &pb.BatchInsertRequest{
			Collection: collection,
			Items:      items[start:end],
		})
		cancel()
		if callErr != nil {
			return callErr
		}
		inserted += resp.Inserted
	}
	fmt.Fprintf(s.out, "batch inserted %d vectors\n", inserted)
	return nil
}

func (s *cliSession) execSearch(cmd cliCmdSearch) error {
	if err := s.ensureConnected(); err != nil {
		return err
	}
	collection, err := s.requireCollection()
	if err != nil {
		return err
	}

	ctx, cancel := s.rpcContext()
	defer cancel()
	resp, err := s.client.Search(ctx, &pb.SearchRequest{
		Collection: collection,
		Embedding:  cmd.Embedding,
		TopK:       uint32(cmd.TopK),
	})
	if err != nil {
		return err
	}

	switch s.format {
	case cliFormatJSON:
		return printJSON(s.out, resp.Results)
	case cliFormatCSV:
		rows := make([][]string, 0, len(resp.Results)+1)
		rows = append(rows, []string{"rank", "key", "score"})
		for i, hit := range resp.Results {
			rows = append(rows, []string{
				strconv.Itoa(i + 1),
				hit.Key,
				strconv.FormatFloat(float64(hit.Score), 'g', 8, 32),
			})
		}
		return printCSVRows(s.out, rows)
	default:
		rows := make([][]string, 0, len(resp.Results))
		for i, hit := range resp.Results {
			rows = append(rows, []string{
				strconv.Itoa(i + 1),
				hit.Key,
				strconv.FormatFloat(float64(hit.Score), 'g', 8, 32),
			})
		}
		printTable(s.out, []string{"rank", "key", "score"}, rows)
		return nil
	}
}

func (s *cliSession) execDelete(cmd cliCmdDelete) error {
	if err := s.ensureConnected(); err != nil {
		return err
	}
	collection, err := s.requireCollection()
	if err != nil {
		return err
	}

	ctx, cancel := s.rpcContext()
	defer cancel()
	_, err = s.client.Delete(ctx, &pb.DeleteRequest{
		Collection: collection,
		Key:        cmd.Key,
	})
	if err != nil {
		return err
	}
	fmt.Fprintf(s.out, "deleted key %s\n", cmd.Key)
	return nil
}

func (s *cliSession) execGet(cmd cliCmdGet) error {
	if err := s.ensureConnected(); err != nil {
		return err
	}
	collection, err := s.requireCollection()
	if err != nil {
		return err
	}

	ctx, cancel := s.rpcContext()
	defer cancel()
	resp, err := s.client.Get(ctx, &pb.GetRequest{
		Collection: collection,
		Key:        cmd.Key,
	})
	if err != nil {
		return err
	}

	switch s.format {
	case cliFormatJSON:
		return printJSON(s.out, resp)
	case cliFormatCSV:
		rows := [][]string{
			{"key", "embedding"},
			{resp.Key, floatSliceToText(resp.Embedding)},
		}
		return printCSVRows(s.out, rows)
	default:
		printTable(s.out, []string{"key", "embedding"}, [][]string{
			{resp.Key, floatSliceToText(resp.Embedding)},
		})
		return nil
	}
}

func (s *cliSession) ensureCollectionExists(name string) error {
	info, err := s.lookupCollection(name)
	if err != nil {
		return err
	}
	if info == nil {
		return fmt.Errorf("not found: collection %s", name)
	}
	return nil
}

func (s *cliSession) lookupCollection(name string) (*pb.CollectionInfo, error) {
	if err := s.ensureConnected(); err != nil {
		return nil, err
	}
	ctx, cancel := s.rpcContext()
	defer cancel()
	resp, err := s.client.ListCollections(ctx, &pb.ListCollectionsRequest{})
	if err != nil {
		return nil, err
	}
	for _, collection := range resp.Collections {
		if strings.EqualFold(collection.Name, name) {
			return collection, nil
		}
	}
	return nil, nil
}

type cliJSONLineItem struct {
	Key       string    `json:"key"`
	Embedding []float32 `json:"embedding"`
}

func (s *cliSession) loadBatchItems(cmd cliCmdBatchInsert) ([]*pb.BatchInsertItem, error) {
	switch cmd.Source {
	case cliBatchSourceFile:
		file, err := os.Open(cmd.Path)
		if err != nil {
			return nil, fmt.Errorf("open batch file %q: %w", cmd.Path, err)
		}
		defer file.Close()
		return parseBatchJSONLines(file)
	case cliBatchSourceStdin:
		fmt.Fprintln(s.out, "Enter JSON lines (one object per line) and finish with \\end:")
		return s.readBatchJSONLinesInteractive()
	default:
		return nil, errors.New("unsupported batch source")
	}
}

func (s *cliSession) readBatchJSONLinesInteractive() ([]*pb.BatchInsertItem, error) {
	items := make([]*pb.BatchInsertItem, 0, 128)
	for {
		fmt.Fprint(s.out, "jsonl> ")
		line, err := s.in.ReadString('\n')
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			if errors.Is(err, io.EOF) {
				break
			}
			continue
		}
		if trimmed == "\\end" {
			break
		}
		item, parseErr := parseBatchJSONLine(trimmed)
		if parseErr != nil {
			return nil, parseErr
		}
		items = append(items, item)
		if errors.Is(err, io.EOF) {
			break
		}
	}
	return items, nil
}

func parseBatchJSONLines(r io.Reader) ([]*pb.BatchInsertItem, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	items := make([]*pb.BatchInsertItem, 0, 512)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		raw := strings.TrimSpace(scanner.Text())
		if raw == "" {
			continue
		}
		item, err := parseBatchJSONLine(raw)
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", lineNo, err)
		}
		items = append(items, item)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func parseBatchJSONLine(raw string) (*pb.BatchInsertItem, error) {
	var line cliJSONLineItem
	if err := json.Unmarshal([]byte(raw), &line); err != nil {
		return nil, fmt.Errorf("invalid json object: %w", err)
	}
	if strings.TrimSpace(line.Key) == "" {
		return nil, errors.New("json line requires non-empty key")
	}
	if len(line.Embedding) == 0 {
		return nil, errors.New("json line requires non-empty embedding")
	}
	return &pb.BatchInsertItem{
		Key:       line.Key,
		Embedding: line.Embedding,
	}, nil
}

func metricFromText(raw string) (pb.Metric, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "cosine":
		return pb.Metric_COSINE, nil
	case "dot", "dot_product":
		return pb.Metric_DOT_PRODUCT, nil
	case "l2":
		return pb.Metric_L2, nil
	default:
		return pb.Metric_COSINE, fmt.Errorf("unsupported metric %q", raw)
	}
}

func rolesToText(roles []security.Role) string {
	out := make([]string, 0, len(roles))
	for _, role := range roles {
		out = append(out, string(role))
	}
	return strings.Join(out, ",")
}

func floatSliceToText(values []float32) string {
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, strconv.FormatFloat(float64(value), 'g', 8, 32))
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func (s *cliSession) resolveUserSecret(literal string, prompt bool) (string, error) {
	if !prompt {
		if strings.TrimSpace(literal) == "" {
			return "", errors.New("api key secret cannot be empty")
		}
		return literal, nil
	}

	secret, err := promptHidden("new api key secret: ")
	if err != nil {
		return "", err
	}
	confirm, err := promptHidden("confirm api key secret: ")
	if err != nil {
		return "", err
	}
	if secret != confirm {
		return "", errors.New("api key secret confirmation does not match")
	}
	if strings.TrimSpace(secret) == "" {
		return "", errors.New("api key secret cannot be empty")
	}
	return secret, nil
}
