package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	notionBaseURL    = "https://api.notion.com/v1"
	notionAPIVersion = "2025-09-03"
)

type Mapping struct {
	Chains map[string]string `json:"chains"`
	Types  map[string]string `json:"types"`
}

// --- Notion: database -> data_sources (pick first) ---
type RetrieveDatabaseResp struct {
	DataSources []struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	} `json:"data_sources"`
}

type QueryResp struct {
	Results    []Page `json:"results"`
	HasMore    bool   `json:"has_more"`
	NextCursor string `json:"next_cursor"`
}

type Page struct {
	ID         string                 `json:"id"`
	Properties map[string]PropertyVal `json:"properties"`
}

type PropertyVal struct {
	Type string `json:"type"`

	Title json.RawMessage `json:"title"`

	Select *struct {
		Name string `json:"name"`
	} `json:"select"`

	MultiSelect []struct {
		Name string `json:"name"`
	} `json:"multi_select"`

	Status *struct {
		Name string `json:"name"`
	} `json:"status"`

	Files []NotionFile `json:"files"`
}

type RichText struct {
	PlainText string `json:"plain_text"`
	Text      struct {
		Content string `json:"content"`
	} `json:"text"`
}

type NotionFile struct {
	Name string `json:"name"`
	Type string `json:"type"`
	File *struct {
		URL        string `json:"url"`
		ExpiryTime string `json:"expiry_time"`
	} `json:"file"`
	External *struct {
		URL string `json:"url"`
	} `json:"external"`
}

type Client struct {
	http          *http.Client
	token         string
	notionVersion string
}

func NewClient(token, version string) *Client {
	return &Client{
		http:          &http.Client{Timeout: 60 * time.Second},
		token:         token,
		notionVersion: version,
	}
}

func (c *Client) do(req *http.Request) (*http.Response, error) {
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Notion-Version", c.notionVersion)
	req.Header.Set("Accept", "application/json")
	return c.http.Do(req)
}

func (c *Client) RetrieveDatabase(ctx context.Context, databaseID string) (RetrieveDatabaseResp, error) {
	var out RetrieveDatabaseResp
	req, err := http.NewRequestWithContext(ctx, "GET", notionBaseURL+"/databases/"+databaseID, nil)
	if err != nil {
		return out, err
	}
	resp, err := c.do(req)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(resp.Body)
		return out, fmt.Errorf("retrieve database failed: %s: %s", resp.Status, string(b))
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return out, err
	}
	return out, nil
}

func (c *Client) QueryDataSource(ctx context.Context, dataSourceID string, body any) (QueryResp, error) {
	var out QueryResp
	b, err := json.Marshal(body)
	if err != nil {
		return out, err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", notionBaseURL+"/data_sources/"+dataSourceID+"/query", bytes.NewReader(b))
	if err != nil {
		return out, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		rb, _ := io.ReadAll(resp.Body)
		return out, fmt.Errorf("query data source failed: %s: %s", resp.Status, string(rb))
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return out, err
	}
	return out, nil
}

func titleText(p PropertyVal) string {
	if len(p.Title) == 0 {
		return ""
	}
	var obj struct {
		Title   []RichText `json:"title"`
		Results []RichText `json:"results"`
	}
	if err := json.Unmarshal(p.Title, &obj); err == nil && (len(obj.Title) > 0 || len(obj.Results) > 0) {
		return joinPlainText(append(obj.Title, obj.Results...))
	}

	var arr []RichText
	if err := json.Unmarshal(p.Title, &arr); err == nil && len(arr) > 0 {
		return joinPlainText(arr)
	}
	return ""
}

func joinPlainText(items []RichText) string {
	parts := make([]string, 0, len(items))
	for _, t := range items {
		if t.PlainText != "" {
			parts = append(parts, t.PlainText)
		}
	}
	return strings.Join(parts, "")
}

type downloadItem struct {
	ChainID    string
	RewardType string
	PageID     string
	SourceURL  string
}

func main() {
	var (
		databaseID    = flag.String("database-id", "", "Notion database ID")
		cycle         = flag.Int("cycle", 0, "Cycle number to fetch (e.g. 20)")
		outDir        = flag.String("out-dir", ".", "Repo root output directory")
		mappingPath   = flag.String("mapping", "config/notion_mappings.json", "JSON mapping file")
		notionToken   = flag.String("notion-token", os.Getenv("NOTION_TOKEN"), "Notion token (or env NOTION_TOKEN)")
		notionVersion = flag.String("notion-version", notionAPIVersion, "Notion API version for Notion-Version header")
		allowExisting = flag.Bool("allow-existing", false, "allow existing cycle directory (re-download and overwrite files)")

		propTitle  = flag.String("prop-title", "Task name", "Title property name")
		propStatus = flag.String("prop-status", "Status", "Status property name")
		propChain  = flag.String("prop-chain", "Chain", "Select property name")
		propType   = flag.String("prop-type", "Type", "Multi-select property name")
		propFile   = flag.String("prop-file", "Merkle file", "Files property name")

		statusDone = flag.String("status-done", "Done", "Status value to match")
		statusType = flag.String("status-type", "status", "Status property type (status or select)")
		pageSize   = flag.Int("page-size", 100, "Notion query page_size")
	)
	flag.Parse()

	if *databaseID == "" || *cycle == 0 {
		fatal(errors.New("missing --database-id or --cycle"))
	}
	if *notionToken == "" {
		fatal(errors.New("missing Notion token (set NOTION_TOKEN or --notion-token)"))
	}
	if *statusType != "status" && *statusType != "select" {
		fatal(fmt.Errorf("invalid --status-type %q (must be status or select)", *statusType))
	}

	var m Mapping
	mb, err := os.ReadFile(*mappingPath)
	if err != nil {
		fatal(fmt.Errorf("read mapping: %w", err))
	}
	if err := json.Unmarshal(mb, &m); err != nil {
		fatal(fmt.Errorf("parse mapping json: %w", err))
	}

	ctx := context.Background()
	cli := NewClient(*notionToken, *notionVersion)

	// Get first data source ID from database (new data model: database -> data_sources)
	db, err := cli.RetrieveDatabase(ctx, *databaseID)
	if err != nil {
		fatal(err)
	}
	if len(db.DataSources) == 0 {
		fatal(errors.New("database has no data_sources"))
	}
	dataSourceID := db.DataSources[0].ID

	cycleStr := fmt.Sprintf("Cycle %d", *cycle)
	targetDir := filepath.Join(*outDir, fmt.Sprintf("cycle-%d", *cycle))

	if fi, err := os.Stat(targetDir); err == nil && fi.IsDir() {
		entries, _ := os.ReadDir(targetDir)
		if len(entries) > 0 && !*allowExisting {
			fatal(fmt.Errorf("target folder %s already exists and is not empty (use --allow-existing)", targetDir))
		}
	}

	statusFilter := map[string]any{"equals": *statusDone}
	filterStatus := map[string]any{
		"property":  *propStatus,
		*statusType: statusFilter,
	}

	body := map[string]any{
		"page_size": *pageSize,
		"filter": map[string]any{
			"and": []any{
				map[string]any{
					"property": *propTitle,
					"title": map[string]any{
						"contains": cycleStr,
					},
				},
				filterStatus,
				map[string]any{
					"property": *propFile,
					"files": map[string]any{
						"is_not_empty": true,
					},
				},
			},
		},
	}

	seen := make(map[string]struct{})
	seenChains := make(map[string]struct{})
	items := make([]downloadItem, 0)

	for {
		qr, err := cli.QueryDataSource(ctx, dataSourceID, body)
		if err != nil {
			fatal(err)
		}

		for _, page := range qr.Results {
			titleProp, ok := page.Properties[*propTitle]
			if !ok || titleProp.Type != "title" {
				fatal(fmt.Errorf("page %s: missing/invalid title property %q", page.ID, *propTitle))
			}
			if !strings.Contains(titleText(titleProp), cycleStr) {
				continue
			}

			chainProp, ok := page.Properties[*propChain]
			if !ok || chainProp.Select == nil || chainProp.Select.Name == "" {
				fatal(fmt.Errorf("page %s: missing chain select %q", page.ID, *propChain))
			}
			chainID, ok := m.Chains[chainProp.Select.Name]
			if !ok {
				fatal(fmt.Errorf("page %s: chain %q not found in mapping", page.ID, chainProp.Select.Name))
			}

			typeProp, ok := page.Properties[*propType]
			if !ok || typeProp.Type != "multi_select" {
				fatal(fmt.Errorf("page %s: missing type multi_select %q", page.ID, *propType))
			}
			if len(typeProp.MultiSelect) != 1 {
				fatal(fmt.Errorf("page %s: expected exactly 1 Type, got %d", page.ID, len(typeProp.MultiSelect)))
			}
			typeName := typeProp.MultiSelect[0].Name
			rewardType, ok := m.Types[typeName]
			if !ok {
				fatal(fmt.Errorf("page %s: type %q not found in mapping", page.ID, typeName))
			}

			fileProp, ok := page.Properties[*propFile]
			if !ok || fileProp.Type != "files" {
				fatal(fmt.Errorf("page %s: missing files property %q", page.ID, *propFile))
			}
			if len(fileProp.Files) != 1 {
				fatal(fmt.Errorf("page %s: expected exactly 1 merkle file, got %d", page.ID, len(fileProp.Files)))
			}
			f := fileProp.Files[0]
			url, err := fileURL(f)
			if err != nil {
				fatal(fmt.Errorf("page %s: %w", page.ID, err))
			}

			key := chainID + ":" + rewardType
			if _, exists := seen[key]; exists {
				fatal(fmt.Errorf("page %s: duplicate chain/type %s", page.ID, key))
			}
			seen[key] = struct{}{}
			seenChains[chainID] = struct{}{}

			items = append(items, downloadItem{
				ChainID:    chainID,
				RewardType: rewardType,
				PageID:     page.ID,
				SourceURL:  url,
			})
		}

		if !qr.HasMore || qr.NextCursor == "" {
			break
		}
		body["start_cursor"] = qr.NextCursor
	}

	if len(items) == 0 {
		fatal(fmt.Errorf("no matching Notion rows found for %s", cycleStr))
	}
	for name, id := range m.Chains {
		if _, ok := seenChains[id]; !ok {
			fatal(fmt.Errorf("no merkle files found for chain %q (id %s) in %s", name, id, cycleStr))
		}
	}

	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		fatal(err)
	}

	for _, item := range items {
		outName := fmt.Sprintf("%s_%s_%d.json", item.ChainID, item.RewardType, *cycle)
		outPath := filepath.Join(targetDir, outName)

		if err := downloadToFile(ctx, cli.http, item.SourceURL, outPath); err != nil {
			fatal(fmt.Errorf("download %s: %w", outName, err))
		}

		if st, err := os.Stat(outPath); err != nil || st.Size() == 0 {
			fatal(fmt.Errorf("downloaded file is empty: %s", outPath))
		}
	}

	fmt.Printf("Downloaded %d files into %s\n", len(items), targetDir)
}

func fileURL(f NotionFile) (string, error) {
	if f.Type == "file" && f.File != nil && f.File.URL != "" {
		return f.File.URL, nil
	}
	if f.Type == "external" && f.External != nil && f.External.URL != "" {
		return f.External.URL, nil
	}
	if f.File != nil && f.File.URL != "" {
		return f.File.URL, nil
	}
	if f.External != nil && f.External.URL != "" {
		return f.External.URL, nil
	}
	return "", fmt.Errorf("file entry %q has no downloadable URL", f.Name)
}

func downloadToFile(ctx context.Context, client *http.Client, urlStr, outPath string) error {
	req, err := http.NewRequestWithContext(ctx, "GET", urlStr, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("download failed: %s: %s", resp.Status, string(b))
	}
	tmp := outPath + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := io.Copy(f, resp.Body); err != nil {
		return err
	}
	return os.Rename(tmp, outPath)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "ERROR:", err.Error())
	os.Exit(1)
}
