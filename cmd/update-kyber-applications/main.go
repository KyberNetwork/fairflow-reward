package main

import (
	"flag"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
)

type pair struct {
	ChainID    string
	RewardType string
}

func main() {
	var (
		valuesPath = flag.String("values", "", "path to core/reward-service/api/public/values.yaml")
		cycleDir   = flag.String("cycle-dir", "", "path to cycle-N directory")
		rawPrefix  = flag.String("raw-prefix", "https://raw.githubusercontent.com/KyberNetwork/fairflow-reward/refs/heads/main", "raw github prefix")
	)
	flag.Parse()
	if *valuesPath == "" || *cycleDir == "" {
		die(fmt.Errorf("missing --values or --cycle-dir"))
	}

	pairs := make(map[pair]struct{})
	cycleNum := 0
	re := regexp.MustCompile(`^([0-9]+)_([A-Za-z]+)_([0-9]+)\.json$`)
	entries, err := os.ReadDir(*cycleDir)
	if err != nil {
		die(err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		m := re.FindStringSubmatch(name)
		if len(m) == 0 {
			continue
		}
		chainID := m[1]
		rewardType := strings.ToUpper(m[2])
		cn, err := strconv.Atoi(m[3])
		if err != nil {
			die(fmt.Errorf("invalid cycle in filename %q: %w", name, err))
		}
		if cycleNum == 0 {
			cycleNum = cn
		} else if cycleNum != cn {
			die(fmt.Errorf("multiple cycle numbers found in %s", *cycleDir))
		}
		pairs[pair{ChainID: chainID, RewardType: rewardType}] = struct{}{}
	}
	if cycleNum == 0 || len(pairs) == 0 {
		die(fmt.Errorf("no matching merkle files found in %s", *cycleDir))
	}
	if cycleNum < 2 {
		die(fmt.Errorf("cycle too small: %d", cycleNum))
	}

	vb, err := os.ReadFile(*valuesPath)
	if err != nil {
		die(err)
	}
	orig := string(vb)
	updated := orig
	changed := false

	newC := cycleNum
	prevC := newC - 1
	oldC := newC - 2

	// replace exact URL substrings.
	for p := range pairs {
		prevURL := fmt.Sprintf("%s/cycle-%d/%s_%s_%d.json", *rawPrefix, prevC, p.ChainID, p.RewardType, prevC)
		newURL := fmt.Sprintf("%s/cycle-%d/%s_%s_%d.json", *rawPrefix, newC, p.ChainID, p.RewardType, newC)
		oldURL := fmt.Sprintf("%s/cycle-%d/%s_%s_%d.json", *rawPrefix, oldC, p.ChainID, p.RewardType, oldC)

		if strings.Contains(updated, prevURL) {
			updated = strings.ReplaceAll(updated, prevURL, newURL)
			changed = true
		}
		if strings.Contains(updated, oldURL) {
			updated = strings.ReplaceAll(updated, oldURL, prevURL)
			changed = true
		}
	}

	if !changed {
		fmt.Println("No changes made to values.yaml (nothing matched).")
		return
	}

	if err := os.WriteFile(*valuesPath, []byte(updated), 0o644); err != nil {
		die(err)
	}
	fmt.Println("Updated values.yaml via URL string replacement only.")
}

func die(err error) {
	fmt.Fprintln(os.Stderr, "ERROR:", err)
	os.Exit(1)
}
