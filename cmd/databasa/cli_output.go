package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func printJSON(w io.Writer, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintln(w, string(data))
	return nil
}

func printCSVRows(w io.Writer, rows [][]string) error {
	writer := csv.NewWriter(w)
	for _, row := range rows {
		if err := writer.Write(row); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

func printTable(w io.Writer, headers []string, rows [][]string) {
	if len(headers) == 0 {
		return
	}
	widths := make([]int, len(headers))
	for i, header := range headers {
		widths[i] = len(header)
	}
	for _, row := range rows {
		for i := 0; i < len(headers) && i < len(row); i++ {
			if len(row[i]) > widths[i] {
				widths[i] = len(row[i])
			}
		}
	}

	render := func(values []string) {
		for i, value := range values {
			if i > 0 {
				fmt.Fprint(w, "  ")
			}
			fmt.Fprint(w, value)
			padding := widths[i] - len(value)
			if padding > 0 {
				fmt.Fprint(w, strings.Repeat(" ", padding))
			}
		}
		fmt.Fprintln(w)
	}

	render(headers)
	separator := make([]string, len(headers))
	for i, width := range widths {
		separator[i] = strings.Repeat("-", width)
	}
	render(separator)
	for _, row := range rows {
		padded := make([]string, len(headers))
		copy(padded, row)
		render(padded)
	}
}

func formatCLIError(err error) string {
	if err == nil {
		return ""
	}

	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.Unauthenticated, codes.PermissionDenied:
			return "unauthorized: " + st.Message()
		case codes.NotFound:
			msg := strings.TrimSpace(st.Message())
			if strings.Contains(strings.ToLower(msg), "collection") {
				if strings.HasPrefix(strings.ToLower(msg), "collection") {
					return "not found: " + msg
				}
				return "not found: collection " + msg
			}
			return "not found: " + msg
		case codes.AlreadyExists:
			return "already exists: " + st.Message()
		case codes.InvalidArgument:
			return "invalid request: " + st.Message()
		case codes.DeadlineExceeded:
			return "timeout: request exceeded configured --timeout"
		case codes.Unavailable:
			return "connection unavailable: " + st.Message()
		default:
			return st.Message()
		}
	}

	msg := err.Error()
	lower := strings.ToLower(msg)
	if strings.Contains(lower, "x509: certificate is valid for") ||
		strings.Contains(lower, "x509: cannot validate certificate for") ||
		strings.Contains(lower, "not valid for any names") {
		return msg + " (TLS hostname/SAN mismatch: use an address present in certificate SANs, provide --ca, or --insecure for local dev)"
	}
	if strings.Contains(lower, "x509: certificate signed by unknown authority") {
		return msg + " (server certificate is not trusted; provide --ca or use --insecure for local development only)"
	}
	return msg
}
