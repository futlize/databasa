package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

type cliCommand interface {
	cliCommandName() string
}

type cliCmdCreateUser struct {
	Name           string
	Password       string
	PromptPassword bool
	Admin          bool
}

func (cliCmdCreateUser) cliCommandName() string { return "create_user" }

type cliCmdAlterUserPassword struct {
	Name           string
	Password       string
	PromptPassword bool
}

func (cliCmdAlterUserPassword) cliCommandName() string { return "alter_user_password" }

type cliCmdDropUser struct {
	Name string
}

func (cliCmdDropUser) cliCommandName() string { return "drop_user" }

type cliCmdListUsers struct{}

func (cliCmdListUsers) cliCommandName() string { return "list_users" }

type cliCmdCreateCollection struct {
	Name   string
	Dim    int
	Metric string
}

func (cliCmdCreateCollection) cliCommandName() string { return "create_collection" }

type cliCmdDropCollection struct {
	Name string
}

func (cliCmdDropCollection) cliCommandName() string { return "drop_collection" }

type cliCmdDescribeCollection struct {
	Name string
}

func (cliCmdDescribeCollection) cliCommandName() string { return "describe_collection" }

type cliCmdListCollections struct{}

func (cliCmdListCollections) cliCommandName() string { return "list_collections" }

type cliCmdUseCollection struct {
	Name string
}

func (cliCmdUseCollection) cliCommandName() string { return "use_collection" }

type cliCmdInsert struct {
	Key       string
	Embedding []float32
}

func (cliCmdInsert) cliCommandName() string { return "insert" }

type cliBatchSource int

const (
	cliBatchSourceFile cliBatchSource = iota
	cliBatchSourceStdin
)

type cliCmdBatchInsert struct {
	Source cliBatchSource
	Path   string
}

func (cliCmdBatchInsert) cliCommandName() string { return "batch_insert" }

type cliCmdSearch struct {
	TopK      int
	Embedding []float32
}

func (cliCmdSearch) cliCommandName() string { return "search" }

type cliCmdDelete struct {
	Key string
}

func (cliCmdDelete) cliCommandName() string { return "delete" }

type cliCmdGet struct {
	Key string
}

func (cliCmdGet) cliCommandName() string { return "get" }

type cliTokenKind int

const (
	cliTokenEOF cliTokenKind = iota
	cliTokenIdent
	cliTokenNumber
	cliTokenString
	cliTokenLBracket
	cliTokenRBracket
	cliTokenComma
	cliTokenSemicolon
)

type cliToken struct {
	kind cliTokenKind
	text string
	pos  int
}

func parseCLICommand(input string) (cliCommand, error) {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return nil, errors.New("empty command")
	}

	tokens, err := tokenizeCLI(trimmed)
	if err != nil {
		return nil, err
	}

	parser := &cliCommandParser{tokens: tokens}
	cmd, err := parser.parse()
	if err != nil {
		return nil, err
	}
	if parser.peek().kind == cliTokenSemicolon {
		parser.next()
	}
	if parser.peek().kind != cliTokenEOF {
		tok := parser.peek()
		return nil, fmt.Errorf("unexpected token %q", tok.text)
	}
	return cmd, nil
}

func tokenizeCLI(input string) ([]cliToken, error) {
	out := make([]cliToken, 0, 32)
	for i := 0; i < len(input); {
		r, size := utf8.DecodeRuneInString(input[i:])
		if r == utf8.RuneError && size == 1 {
			return nil, fmt.Errorf("invalid utf-8 at byte %d", i)
		}
		if unicode.IsSpace(r) {
			i += size
			continue
		}

		switch r {
		case '[':
			out = append(out, cliToken{kind: cliTokenLBracket, text: "[", pos: i})
			i += size
		case ']':
			out = append(out, cliToken{kind: cliTokenRBracket, text: "]", pos: i})
			i += size
		case ',':
			out = append(out, cliToken{kind: cliTokenComma, text: ",", pos: i})
			i += size
		case ';':
			out = append(out, cliToken{kind: cliTokenSemicolon, text: ";", pos: i})
			i += size
		case '\'':
			start := i
			i += size
			var b strings.Builder
			for i < len(input) {
				cr, csize := utf8.DecodeRuneInString(input[i:])
				if cr == utf8.RuneError && csize == 1 {
					return nil, fmt.Errorf("invalid utf-8 in string at byte %d", i)
				}
				if cr == '\\' {
					i += csize
					if i >= len(input) {
						return nil, fmt.Errorf("unterminated escape sequence in string at byte %d", start)
					}
					er, esize := utf8.DecodeRuneInString(input[i:])
					switch er {
					case '\'', '\\':
						b.WriteRune(er)
					default:
						b.WriteRune('\\')
						b.WriteRune(er)
					}
					i += esize
					continue
				}
				if cr == '\'' {
					i += csize
					out = append(out, cliToken{kind: cliTokenString, text: b.String(), pos: start})
					goto nextToken
				}
				b.WriteRune(cr)
				i += csize
			}
			return nil, fmt.Errorf("unterminated string literal at byte %d", start)
		default:
			if isCLIWordRune(r, true) {
				start := i
				i += size
				for i < len(input) {
					nr, nsize := utf8.DecodeRuneInString(input[i:])
					if !isCLIWordRune(nr, false) {
						break
					}
					i += nsize
				}
				word := input[start:i]
				if isCLINumberLiteral(word) {
					out = append(out, cliToken{kind: cliTokenNumber, text: word, pos: start})
				} else {
					out = append(out, cliToken{kind: cliTokenIdent, text: word, pos: start})
				}
				continue
			}
			return nil, fmt.Errorf("unexpected character %q at byte %d", r, i)
		}
	nextToken:
	}
	out = append(out, cliToken{kind: cliTokenEOF, pos: len(input)})
	return out, nil
}

func isCLIWordRune(r rune, first bool) bool {
	if unicode.IsLetter(r) || unicode.IsDigit(r) {
		return true
	}
	switch r {
	case '_', '-', '.', ':':
		return true
	case '+':
		return first
	}
	return false
}

func isCLINumberLiteral(raw string) bool {
	if raw == "" {
		return false
	}
	if raw == "." || raw == "+" || raw == "-" {
		return false
	}
	if _, err := strconv.ParseFloat(raw, 64); err != nil {
		return false
	}
	return true
}

type cliCommandParser struct {
	tokens []cliToken
	pos    int
}

func (p *cliCommandParser) parse() (cliCommand, error) {
	switch {
	case p.consumeKeyword("CREATE"):
		return p.parseCreate()
	case p.consumeKeyword("ALTER"):
		return p.parseAlter()
	case p.consumeKeyword("DROP"):
		return p.parseDrop()
	case p.consumeKeyword("LIST"):
		return p.parseList()
	case p.consumeKeyword("DESCRIBE"):
		return p.parseDescribe()
	case p.consumeKeyword("USE"):
		name, err := p.expectName()
		if err != nil {
			return nil, err
		}
		return cliCmdUseCollection{Name: name}, nil
	case p.consumeKeyword("INSERT"):
		return p.parseInsert()
	case p.consumeKeyword("BATCH"):
		return p.parseBatchInsert()
	case p.consumeKeyword("SEARCH"):
		return p.parseSearch()
	case p.consumeKeyword("DELETE"):
		return p.parseDelete()
	case p.consumeKeyword("GET"):
		return p.parseGet()
	default:
		tok := p.peek()
		return nil, fmt.Errorf("unknown command near %q", tok.text)
	}
}

func (p *cliCommandParser) parseCreate() (cliCommand, error) {
	switch {
	case p.consumeKeyword("USER"):
		name, err := p.expectName()
		if err != nil {
			return nil, err
		}
		if err := p.expectKeyword("PASSWORD"); err != nil {
			return nil, err
		}
		password, promptPassword, err := p.parseOptionalPasswordLiteral()
		if err != nil {
			return nil, err
		}
		admin := p.consumeKeyword("ADMIN")
		return cliCmdCreateUser{
			Name:           name,
			Password:       password,
			PromptPassword: promptPassword,
			Admin:          admin,
		}, nil
	case p.consumeKeyword("COLLECTION"):
		name, err := p.expectName()
		if err != nil {
			return nil, err
		}
		if err := p.expectKeyword("DIM"); err != nil {
			return nil, err
		}
		dim, err := p.expectInt()
		if err != nil {
			return nil, err
		}
		metric := "cosine"
		if p.consumeKeyword("METRIC") {
			val, err := p.expectKeywordValue()
			if err != nil {
				return nil, err
			}
			metric = strings.ToLower(val)
			switch metric {
			case "cosine", "dot", "dot_product", "l2":
			default:
				return nil, fmt.Errorf("unsupported metric %q", val)
			}
		}
		return cliCmdCreateCollection{Name: name, Dim: dim, Metric: metric}, nil
	default:
		return nil, fmt.Errorf("unsupported CREATE statement near %q", p.peek().text)
	}
}

func (p *cliCommandParser) parseAlter() (cliCommand, error) {
	if !p.consumeKeyword("USER") {
		return nil, fmt.Errorf("unsupported ALTER statement near %q", p.peek().text)
	}
	name, err := p.expectName()
	if err != nil {
		return nil, err
	}
	if err := p.expectKeyword("PASSWORD"); err != nil {
		return nil, err
	}
	password, promptPassword, err := p.parseOptionalPasswordLiteral()
	if err != nil {
		return nil, err
	}
	return cliCmdAlterUserPassword{
		Name:           name,
		Password:       password,
		PromptPassword: promptPassword,
	}, nil
}

func (p *cliCommandParser) parseDrop() (cliCommand, error) {
	switch {
	case p.consumeKeyword("USER"):
		name, err := p.expectName()
		if err != nil {
			return nil, err
		}
		return cliCmdDropUser{Name: name}, nil
	case p.consumeKeyword("COLLECTION"):
		name, err := p.expectName()
		if err != nil {
			return nil, err
		}
		return cliCmdDropCollection{Name: name}, nil
	default:
		return nil, fmt.Errorf("unsupported DROP statement near %q", p.peek().text)
	}
}

func (p *cliCommandParser) parseList() (cliCommand, error) {
	switch {
	case p.consumeKeyword("USERS"):
		return cliCmdListUsers{}, nil
	case p.consumeKeyword("COLLECTIONS"):
		return cliCmdListCollections{}, nil
	default:
		return nil, fmt.Errorf("unsupported LIST statement near %q", p.peek().text)
	}
}

func (p *cliCommandParser) parseDescribe() (cliCommand, error) {
	if !p.consumeKeyword("COLLECTION") {
		return nil, fmt.Errorf("unsupported DESCRIBE statement near %q", p.peek().text)
	}
	name, err := p.expectName()
	if err != nil {
		return nil, err
	}
	return cliCmdDescribeCollection{Name: name}, nil
}

func (p *cliCommandParser) parseInsert() (cliCommand, error) {
	if err := p.expectKeyword("KEY"); err != nil {
		return nil, err
	}
	key, err := p.expectString()
	if err != nil {
		return nil, err
	}
	if err := p.expectKeyword("EMBEDDING"); err != nil {
		return nil, err
	}
	embedding, err := p.parseVector()
	if err != nil {
		return nil, err
	}
	return cliCmdInsert{Key: key, Embedding: embedding}, nil
}

func (p *cliCommandParser) parseBatchInsert() (cliCommand, error) {
	if err := p.expectKeyword("INSERT"); err != nil {
		return nil, err
	}
	_ = p.consumeKeyword("FROM")
	switch {
	case p.consumeKeyword("FILE"):
		path, err := p.expectString()
		if err != nil {
			return nil, err
		}
		return cliCmdBatchInsert{Source: cliBatchSourceFile, Path: path}, nil
	case p.consumeKeyword("STDIN"):
		return cliCmdBatchInsert{Source: cliBatchSourceStdin}, nil
	default:
		return nil, fmt.Errorf("BATCH INSERT expects FILE '<path>' or STDIN")
	}
}

func (p *cliCommandParser) parseSearch() (cliCommand, error) {
	if err := p.expectKeyword("TOPK"); err != nil {
		return nil, err
	}
	topK, err := p.expectInt()
	if err != nil {
		return nil, err
	}
	if err := p.expectKeyword("EMBEDDING"); err != nil {
		return nil, err
	}
	embedding, err := p.parseVector()
	if err != nil {
		return nil, err
	}
	return cliCmdSearch{TopK: topK, Embedding: embedding}, nil
}

func (p *cliCommandParser) parseDelete() (cliCommand, error) {
	if err := p.expectKeyword("KEY"); err != nil {
		return nil, err
	}
	key, err := p.expectString()
	if err != nil {
		return nil, err
	}
	return cliCmdDelete{Key: key}, nil
}

func (p *cliCommandParser) parseGet() (cliCommand, error) {
	if err := p.expectKeyword("KEY"); err != nil {
		return nil, err
	}
	key, err := p.expectString()
	if err != nil {
		return nil, err
	}
	return cliCmdGet{Key: key}, nil
}

func (p *cliCommandParser) parseVector() ([]float32, error) {
	if p.peek().kind != cliTokenLBracket {
		return nil, fmt.Errorf("expected '[' to start embedding")
	}
	p.next()

	out := make([]float32, 0, 16)
	if p.peek().kind == cliTokenRBracket {
		p.next()
		return out, nil
	}
	for {
		value, err := p.expectFloat32()
		if err != nil {
			return nil, err
		}
		out = append(out, value)
		if p.peek().kind == cliTokenComma {
			p.next()
			continue
		}
		if p.peek().kind == cliTokenRBracket {
			p.next()
			break
		}
		return nil, fmt.Errorf("expected ',' or ']' in embedding")
	}
	return out, nil
}

func (p *cliCommandParser) expectKeyword(value string) error {
	if p.consumeKeyword(value) {
		return nil
	}
	return fmt.Errorf("expected keyword %q near %q", value, p.peek().text)
}

func (p *cliCommandParser) consumeKeyword(value string) bool {
	tok := p.peek()
	if tok.kind != cliTokenIdent {
		return false
	}
	if !strings.EqualFold(tok.text, value) {
		return false
	}
	p.next()
	return true
}

func (p *cliCommandParser) expectKeywordValue() (string, error) {
	tok := p.peek()
	if tok.kind != cliTokenIdent {
		return "", fmt.Errorf("expected keyword near %q", tok.text)
	}
	p.next()
	return tok.text, nil
}

func (p *cliCommandParser) expectName() (string, error) {
	tok := p.peek()
	switch tok.kind {
	case cliTokenIdent, cliTokenString:
		p.next()
		return tok.text, nil
	default:
		return "", fmt.Errorf("expected name near %q", tok.text)
	}
}

func (p *cliCommandParser) expectString() (string, error) {
	tok := p.peek()
	if tok.kind != cliTokenString {
		return "", fmt.Errorf("expected quoted string near %q", tok.text)
	}
	p.next()
	return tok.text, nil
}

func (p *cliCommandParser) parseOptionalPasswordLiteral() (string, bool, error) {
	if p.peek().kind == cliTokenString {
		return "", false, errors.New("inline PASSWORD literal is disabled; omit value after PASSWORD to enter secret securely")
	}
	return "", true, nil
}

func (p *cliCommandParser) expectInt() (int, error) {
	tok := p.peek()
	if tok.kind != cliTokenNumber && tok.kind != cliTokenIdent {
		return 0, fmt.Errorf("expected integer near %q", tok.text)
	}
	v, err := strconv.Atoi(tok.text)
	if err != nil {
		return 0, fmt.Errorf("expected integer near %q", tok.text)
	}
	p.next()
	return v, nil
}

func (p *cliCommandParser) expectFloat32() (float32, error) {
	tok := p.peek()
	if tok.kind != cliTokenNumber && tok.kind != cliTokenIdent {
		return 0, fmt.Errorf("expected float near %q", tok.text)
	}
	v, err := strconv.ParseFloat(tok.text, 32)
	if err != nil {
		return 0, fmt.Errorf("expected float near %q", tok.text)
	}
	p.next()
	return float32(v), nil
}

func (p *cliCommandParser) peek() cliToken {
	if p.pos >= len(p.tokens) {
		return cliToken{kind: cliTokenEOF}
	}
	return p.tokens[p.pos]
}

func (p *cliCommandParser) next() cliToken {
	tok := p.peek()
	if p.pos < len(p.tokens) {
		p.pos++
	}
	return tok
}

func cliStatementComplete(input string) (bool, int, error) {
	inSingle := false
	inDouble := false
	escaped := false
	depth := 0

	for i, r := range input {
		if escaped {
			escaped = false
			continue
		}

		if inSingle {
			if r == '\\' {
				escaped = true
				continue
			}
			if r == '\'' {
				inSingle = false
			}
			continue
		}
		if inDouble {
			if r == '\\' {
				escaped = true
				continue
			}
			if r == '"' {
				inDouble = false
			}
			continue
		}

		switch r {
		case '\'':
			inSingle = true
		case '"':
			inDouble = true
		case '[':
			depth++
		case ']':
			depth--
			if depth < 0 {
				return false, -1, errors.New("unexpected ']' before '['")
			}
		case ';':
			if depth == 0 {
				return true, i, nil
			}
		}
	}
	return false, -1, nil
}
