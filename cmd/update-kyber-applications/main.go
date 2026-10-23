package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type pair struct {
	ChainID    string
	RewardType string
}

// cyclesKept is how many cycle files a campaign carries. A cycle file is
// published a few hours before its own startTimestamp and reward-service serves
// the newest cycle that has already started, so the previous file has to stay
// behind the new one or the campaign drops out of the API until the window
// opens.
const cyclesKept = 2

// Campaigns are matched by distributor, never by position: merkle leaves hash
// the campaign id, so a file published for one campaign fails proof
// verification under any other, and a deprecated distributor's campaigns are
// frozen the moment it stops accruing rewards.
type config struct {
	Chains map[string]chainConfig `json:"chains"`
}

type chainConfig struct {
	Distributors []distributor       `json:"distributors"`
	Campaigns    map[string]campaign `json:"campaigns"`
}

type distributor struct {
	Address string `json:"address"`
	Status  string `json:"status"`
}

type campaign struct {
	Type        string  `json:"type"`
	Distributor string  `json:"distributor"`
	Cycles      []cycle `json:"cycles"`
}

type cycle struct {
	FilePath string `json:"filepath"`
}

func main() {
	var (
		valuesPath = flag.String("values", "", "path to the kyber-applications reward-service values.yaml")
		cycleDir   = flag.String("cycle-dir", "", "path to cycle-N directory")
		rawPrefix  = flag.String("raw-prefix", "https://raw.githubusercontent.com/KyberNetwork/fairflow-reward/refs/heads/main", "raw github prefix")
		dryRun     = flag.Bool("dry-run", false, "report the planned changes without writing")
	)
	flag.Parse()
	if *valuesPath == "" || *cycleDir == "" {
		die(fmt.Errorf("missing --values or --cycle-dir"))
	}

	pairs, cycleNum, err := scanCycleDir(*cycleDir)
	if err != nil {
		die(err)
	}

	original, err := os.ReadFile(*valuesPath)
	if err != nil {
		die(err)
	}

	updated, plan, err := updateValues(string(original), pairs, cycleNum, *rawPrefix)
	if err != nil {
		die(err)
	}
	for _, line := range plan {
		fmt.Println(line)
	}

	if updated == string(original) {
		fmt.Println("No changes made to values.yaml (already up to date).")
		return
	}
	if *dryRun {
		fmt.Println("Dry run: values.yaml left untouched.")
		return
	}
	if err := os.WriteFile(*valuesPath, []byte(updated), 0o644); err != nil {
		die(err)
	}
	fmt.Printf("Updated values.yaml for cycle %d.\n", cycleNum)
}

var cycleFileRe = regexp.MustCompile(`^([0-9]+)_([A-Za-z]+)_([0-9]+)\.json$`)

func scanCycleDir(dir string) ([]pair, int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, 0, err
	}

	seen := make(map[pair]struct{})
	cycleNum := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		m := cycleFileRe.FindStringSubmatch(entry.Name())
		if len(m) == 0 {
			continue
		}
		cn, err := strconv.Atoi(m[3])
		if err != nil {
			return nil, 0, fmt.Errorf("invalid cycle in filename %q: %w", entry.Name(), err)
		}
		if cycleNum == 0 {
			cycleNum = cn
		} else if cycleNum != cn {
			return nil, 0, fmt.Errorf("multiple cycle numbers found in %s", dir)
		}
		seen[pair{ChainID: m[1], RewardType: strings.ToUpper(m[2])}] = struct{}{}
	}
	if cycleNum == 0 || len(seen) == 0 {
		return nil, 0, fmt.Errorf("no matching merkle files found in %s", dir)
	}
	if cycleNum < 2 {
		return nil, 0, fmt.Errorf("cycle too small: %d", cycleNum)
	}

	pairs := make([]pair, 0, len(seen))
	for p := range seen {
		pairs = append(pairs, p)
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].ChainID != pairs[j].ChainID {
			return pairs[i].ChainID < pairs[j].ChainID
		}
		return pairs[i].RewardType < pairs[j].RewardType
	})
	return pairs, cycleNum, nil
}

// updateValues resolves every target before editing anything, so a config that
// cannot be resolved leaves values.yaml untouched instead of half-updated.
func updateValues(valuesText string, pairs []pair, cycleNum int, rawPrefix string) (string, []string, error) {
	block, err := findJSONBlock(valuesText)
	if err != nil {
		return "", nil, err
	}

	var cfg config
	if err := json.Unmarshal([]byte(block.text), &cfg); err != nil {
		return "", nil, fmt.Errorf("DistributorCampaigns is not valid JSON: %w", err)
	}
	locations, err := locateCycles(block.text)
	if err != nil {
		return "", nil, err
	}

	type edit struct {
		start, end int
		text       string
	}
	var (
		edits []edit
		plan  []string
	)

	for _, p := range pairs {
		chain, ok := cfg.Chains[p.ChainID]
		if !ok {
			return "", nil, fmt.Errorf("cycle %d has a %s file for chain %s, which is not configured in DistributorCampaigns",
				cycleNum, p.RewardType, p.ChainID)
		}
		active, err := chain.activeDistributor()
		if err != nil {
			return "", nil, fmt.Errorf("chain %s: %w", p.ChainID, err)
		}
		campaignID, target, err := chain.activeCampaign(p.RewardType, active)
		if err != nil {
			return "", nil, fmt.Errorf("chain %s: %w", p.ChainID, err)
		}
		if err := checkExistingCycles(target, p, rawPrefix); err != nil {
			return "", nil, fmt.Errorf("chain %s campaign %s: %w", p.ChainID, campaignID, err)
		}

		newURL := cycleURL(rawPrefix, p, cycleNum)
		if len(target.Cycles) > 0 && target.Cycles[0].FilePath == newURL {
			plan = append(plan, fmt.Sprintf("chain %s %s: campaign %s already at cycle %d",
				p.ChainID, p.RewardType, campaignID, cycleNum))
			continue
		}

		urls := []string{newURL}
		for _, existing := range target.Cycles {
			if len(urls) == cyclesKept {
				break
			}
			urls = append(urls, existing.FilePath)
		}

		loc, ok := locations[[2]string{p.ChainID, campaignID}]
		if !ok {
			return "", nil, fmt.Errorf("chain %s campaign %s: no cycles array found", p.ChainID, campaignID)
		}
		edits = append(edits, edit{start: loc.start, end: loc.end, text: renderCycles(block.text, loc.start, urls)})
		plan = append(plan, fmt.Sprintf("chain %s %s: campaign %s -> %s",
			p.ChainID, p.RewardType, campaignID, strings.Join(shortNames(urls), ", ")))
	}

	// Apply back to front so earlier offsets stay valid.
	sort.Slice(edits, func(i, j int) bool { return edits[i].start > edits[j].start })
	updatedJSON := block.text
	for _, e := range edits {
		updatedJSON = updatedJSON[:e.start] + e.text + updatedJSON[e.end:]
	}

	return block.splice(valuesText, updatedJSON), plan, nil
}

func (c chainConfig) activeDistributor() (string, error) {
	var active []string
	for _, d := range c.Distributors {
		if strings.EqualFold(d.Status, "active") {
			active = append(active, d.Address)
		}
	}
	switch len(active) {
	case 1:
		return active[0], nil
	case 0:
		return "", fmt.Errorf("no distributor with status \"active\"")
	default:
		return "", fmt.Errorf("%d distributors with status \"active\" (%s); exactly one must be active",
			len(active), strings.Join(active, ", "))
	}
}

// activeCampaign returns the one campaign of this reward type pinned to the
// active distributor. A campaign with no "distributor" falls back to the
// chain-level contractAddress, which is the superseded contract, so it is never
// a target.
func (c chainConfig) activeCampaign(rewardType, active string) (string, campaign, error) {
	var ids []string
	for id, candidate := range c.Campaigns {
		if !strings.EqualFold(candidate.Type, rewardType) {
			continue
		}
		if candidate.Distributor == "" || !strings.EqualFold(candidate.Distributor, active) {
			continue
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)

	switch len(ids) {
	case 1:
		return ids[0], c.Campaigns[ids[0]], nil
	case 0:
		return "", campaign{}, fmt.Errorf("no %s campaign pinned to the active distributor %s; add the campaign id created on that contract before syncing",
			rewardType, active)
	default:
		return "", campaign{}, fmt.Errorf("%d %s campaigns pinned to the active distributor %s (%s); exactly one must be",
			len(ids), rewardType, active, strings.Join(ids, ", "))
	}
}

// checkExistingCycles refuses to edit a campaign whose cycle files are not this
// chain and reward type, which would mean the campaign was matched wrongly.
func checkExistingCycles(c campaign, p pair, rawPrefix string) error {
	want := regexp.MustCompile(fmt.Sprintf(`^%s/cycle-([0-9]+)/%s_%s_([0-9]+)\.json$`,
		regexp.QuoteMeta(rawPrefix), regexp.QuoteMeta(p.ChainID), regexp.QuoteMeta(p.RewardType)))
	for _, existing := range c.Cycles {
		m := want.FindStringSubmatch(existing.FilePath)
		if len(m) == 0 {
			return fmt.Errorf("cycle file %q is not a %s %s file under %s", existing.FilePath, p.ChainID, p.RewardType, rawPrefix)
		}
		if m[1] != m[2] {
			return fmt.Errorf("cycle file %q mixes cycle %s and %s", existing.FilePath, m[1], m[2])
		}
	}
	return nil
}

func cycleURL(rawPrefix string, p pair, cycleNum int) string {
	return fmt.Sprintf("%s/cycle-%d/%s_%s_%d.json", rawPrefix, cycleNum, p.ChainID, p.RewardType, cycleNum)
}

func shortNames(urls []string) []string {
	out := make([]string, 0, len(urls))
	for _, u := range urls {
		parts := strings.Split(u, "/")
		if len(parts) >= 2 {
			out = append(out, strings.Join(parts[len(parts)-2:], "/"))
			continue
		}
		out = append(out, u)
	}
	return out
}

// renderCycles writes the array in the layout the file already uses: the
// opening bracket stays on the key line, one element per line indented two
// further, closing bracket back at the key's indentation.
func renderCycles(jsonText string, start int, urls []string) string {
	lineStart := strings.LastIndexByte(jsonText[:start], '\n') + 1
	keyIndent := jsonText[lineStart : lineStart+indentWidth(jsonText[lineStart:])]
	elemIndent := keyIndent + "  "

	var b strings.Builder
	b.WriteString("[\n")
	for i, u := range urls {
		b.WriteString(elemIndent)
		fmt.Fprintf(&b, "{ \"filepath\": %q }", u)
		if i < len(urls)-1 {
			b.WriteString(",")
		}
		b.WriteString("\n")
	}
	b.WriteString(keyIndent)
	b.WriteString("]")
	return b.String()
}

func indentWidth(s string) int {
	for i := 0; i < len(s); i++ {
		if s[i] != ' ' && s[i] != '\t' {
			return i
		}
	}
	return len(s)
}

// jsonBlock is the DistributorCampaigns block scalar, dedented to valid JSON.
type jsonBlock struct {
	text   string // dedented JSON
	indent string // indentation stripped from every line
	start  int    // byte offset of the block in values.yaml
	end    int
}

var blockKeyRe = regexp.MustCompile(`(?m)^[ \t]*DistributorCampaigns: \|[ \t]*\r?\n`)

func findJSONBlock(valuesText string) (jsonBlock, error) {
	matches := blockKeyRe.FindAllStringIndex(valuesText, -1)
	if len(matches) != 1 {
		return jsonBlock{}, fmt.Errorf("expected exactly one \"DistributorCampaigns: |\" block, found %d", len(matches))
	}
	start := matches[0][1]

	lines := strings.SplitAfter(valuesText[start:], "\n")
	if len(lines) == 0 {
		return jsonBlock{}, fmt.Errorf("DistributorCampaigns block is empty")
	}
	indent := lines[0][:indentWidth(lines[0])]
	if indent == "" {
		return jsonBlock{}, fmt.Errorf("DistributorCampaigns block is not indented")
	}

	var (
		dedented strings.Builder
		consumed int
	)
	for _, line := range lines {
		trimmed := strings.TrimRight(line, "\r\n")
		if trimmed != "" && !strings.HasPrefix(line, indent) {
			break
		}
		dedented.WriteString(strings.TrimPrefix(line, indent))
		consumed += len(line)
	}

	block := jsonBlock{text: dedented.String(), indent: indent, start: start, end: start + consumed}
	// A block that does not round-trip would be rewritten with different
	// whitespace, so refuse it rather than reformat the file.
	if block.splice(valuesText, block.text) != valuesText {
		return jsonBlock{}, fmt.Errorf("DistributorCampaigns block uses indentation this tool cannot preserve")
	}
	return block, nil
}

func (b jsonBlock) splice(valuesText, updatedJSON string) string {
	var indented strings.Builder
	for _, line := range strings.SplitAfter(updatedJSON, "\n") {
		if strings.TrimRight(line, "\r\n") != "" {
			indented.WriteString(b.indent)
		}
		indented.WriteString(line)
	}
	return valuesText[:b.start] + indented.String() + valuesText[b.end:]
}

type span struct {
	start, end int
}

type frame struct {
	isObject  bool
	openedAs  string // key this container was the value of
	key       string // key whose value is being read
	expectKey bool
	tracking  bool // this array is a cycles array being measured
	chainID   string
	campaign  string
	start     int
}

// locateCycles finds the byte span of every chains.<id>.campaigns.<id>.cycles
// array, so the arrays can be rewritten in place without reserializing (and
// reordering) the whole config.
func locateCycles(jsonText string) (map[[2]string]span, error) {
	dec := json.NewDecoder(strings.NewReader(jsonText))
	out := make(map[[2]string]span)

	var stack []*frame
	pending := false

	valueDone := func() {
		if len(stack) > 0 && stack[len(stack)-1].isObject {
			stack[len(stack)-1].expectKey = true
		}
	}

	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("walking DistributorCampaigns: %w", err)
		}

		if delim, ok := tok.(json.Delim); ok {
			switch delim {
			case '}', ']':
				done := stack[len(stack)-1]
				stack = stack[:len(stack)-1]
				if done.tracking {
					out[[2]string{done.chainID, done.campaign}] = span{start: done.start, end: int(dec.InputOffset())}
				}
				valueDone()
				continue
			case '{':
				openedAs := ""
				if len(stack) > 0 {
					openedAs = stack[len(stack)-1].key
				}
				stack = append(stack, &frame{isObject: true, openedAs: openedAs, expectKey: true})
				pending = false
				continue
			case '[':
				openedAs := ""
				if len(stack) > 0 {
					openedAs = stack[len(stack)-1].key
				}
				next := &frame{openedAs: openedAs}
				if pending {
					chainID, campaignID, _ := campaignPath(stack)
					next.tracking = true
					next.chainID = chainID
					next.campaign = campaignID
					next.start = int(dec.InputOffset()) - 1
					pending = false
				}
				stack = append(stack, next)
				continue
			}
		}

		if len(stack) > 0 && stack[len(stack)-1].isObject && stack[len(stack)-1].expectKey {
			key, ok := tok.(string)
			if !ok {
				return nil, fmt.Errorf("walking DistributorCampaigns: expected object key, got %v", tok)
			}
			top := stack[len(stack)-1]
			top.key = key
			top.expectKey = false
			if key == "cycles" {
				if _, _, ok := campaignPath(stack); ok {
					pending = true
				}
			}
			continue
		}

		if pending {
			return nil, fmt.Errorf("walking DistributorCampaigns: cycles must be an array, got %v", tok)
		}
		valueDone()
	}

	return out, nil
}

// campaignPath reports the chain and campaign ids when the innermost object is
// chains.<chainID>.campaigns.<campaignID>.
func campaignPath(stack []*frame) (string, string, bool) {
	top := len(stack) - 1
	if top < 3 {
		return "", "", false
	}
	if stack[top-1].openedAs != "campaigns" || stack[top-3].openedAs != "chains" {
		return "", "", false
	}
	return stack[top-2].openedAs, stack[top].openedAs, true
}

func die(err error) {
	fmt.Fprintln(os.Stderr, "ERROR:", err)
	os.Exit(1)
}
