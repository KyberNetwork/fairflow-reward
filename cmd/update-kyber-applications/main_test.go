package main

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

const testPrefix = "https://raw.example/fairflow-reward/main"

// fixture mirrors the real values.yaml shape: the JSON config is a block scalar
// nested deep in the chart values, with a sibling key after it.
const fixture = `base-service:
  server:
    config: |
      kem:
        DistributorCampaigns: |
          {
            "chains": {
              "8453": {
                "chainName": "base",
                "contractAddress": "0xOLD",
                "distributors": [
                  { "address": "0xOLD", "version": 1, "status": "deprecated" },
                  { "address": "0xNEW", "version": 2, "status": "active" }
                ],
                "campaigns": {
                  "0xactive": {
                    "type": "EG",
                    "distributor": "0xnew",
                    "cycles": [
                      { "filepath": "PREFIX/cycle-62/8453_EG_62.json" }
                    ],
                    "waiting": []
                  },
                  "0xdeprecated": {
                    "type": "EG",
                    "cycles": [
                      { "filepath": "PREFIX/cycle-61/8453_EG_61.json" },
                      { "filepath": "PREFIX/cycle-60/8453_EG_60.json" }
                    ]
                  },
                  "0xplaceholder": {
                    "type": "LM",
                    "cycles": [
                      { "filepath": "https://gist.example/empty.json" }
                    ]
                  }
                }
              }
            }
          }
        CacheTTLSeconds: 60
`

func values(t *testing.T) string {
	t.Helper()
	return strings.ReplaceAll(fixture, "PREFIX", testPrefix)
}

func campaigns(t *testing.T, valuesText string) map[string]campaign {
	t.Helper()
	block, err := findJSONBlock(valuesText)
	if err != nil {
		t.Fatalf("findJSONBlock: %v", err)
	}
	var cfg config
	if err := json.Unmarshal([]byte(block.text), &cfg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return cfg.Chains["8453"].Campaigns
}

func filepaths(c campaign) []string {
	out := make([]string, 0, len(c.Cycles))
	for _, cycle := range c.Cycles {
		out = append(out, cycle.FilePath)
	}
	return out
}

var basePair = pair{ChainID: "8453", RewardType: "EG"}

// The new cycle must land on the campaign pinned to the active distributor and
// keep the previous file as the fallback: a file published for one campaign
// fails merkle verification under any other, and the newest file is not yet
// claimable when its startTimestamp is still in the future.
func TestUpdateValuesPrependsToActiveCampaign(t *testing.T) {
	original := values(t)

	updated, plan, err := updateValues(original, []pair{basePair}, 63, testPrefix)
	if err != nil {
		t.Fatalf("updateValues: %v", err)
	}

	got := campaigns(t, updated)
	want := []string{
		testPrefix + "/cycle-63/8453_EG_63.json",
		testPrefix + "/cycle-62/8453_EG_62.json",
	}
	if diff := filepaths(got["0xactive"]); !reflect.DeepEqual(diff, want) {
		t.Errorf("active campaign cycles = %v, want %v", diff, want)
	}

	before := campaigns(t, original)
	for _, id := range []string{"0xdeprecated", "0xplaceholder"} {
		if !reflect.DeepEqual(before[id], got[id]) {
			t.Errorf("campaign %s changed: %v -> %v", id, filepaths(before[id]), filepaths(got[id]))
		}
	}
	if len(plan) != 1 || !strings.Contains(plan[0], "0xactive") {
		t.Errorf("plan = %v, want one line naming the active campaign", plan)
	}
	if !strings.HasSuffix(updated, "        CacheTTLSeconds: 60\n") {
		t.Error("content after the block was not preserved")
	}
}

func TestUpdateValuesIsIdempotent(t *testing.T) {
	once, _, err := updateValues(values(t), []pair{basePair}, 63, testPrefix)
	if err != nil {
		t.Fatalf("first run: %v", err)
	}
	twice, plan, err := updateValues(once, []pair{basePair}, 63, testPrefix)
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	if twice != once {
		t.Error("second run changed the file again")
	}
	if len(plan) != 1 || !strings.Contains(plan[0], "already at cycle 63") {
		t.Errorf("plan = %v, want an already-up-to-date line", plan)
	}
}

// A campaign that only ever accrued on the superseded contract has no
// "distributor" pin, so a reward type with no pinned campaign has nowhere safe
// to go and must stop the run instead of landing on the deprecated campaign.
func TestUpdateValuesRejectsUnpinnedRewardType(t *testing.T) {
	updated, _, err := updateValues(values(t), []pair{{ChainID: "8453", RewardType: "LM"}}, 63, testPrefix)
	if err == nil {
		t.Fatal("expected an error for a reward type with no pinned campaign")
	}
	if !strings.Contains(err.Error(), "no LM campaign pinned") {
		t.Errorf("error = %q, want it to name the missing pinned campaign", err)
	}
	if updated != "" {
		t.Error("values text was returned despite the error")
	}
}

func TestUpdateValuesRejectsAmbiguousActiveDistributor(t *testing.T) {
	ambiguous := strings.Replace(values(t), `"version": 1, "status": "deprecated"`, `"version": 1, "status": "active"`, 1)

	if _, _, err := updateValues(ambiguous, []pair{basePair}, 63, testPrefix); err == nil ||
		!strings.Contains(err.Error(), "exactly one must be active") {
		t.Errorf("error = %v, want a complaint about two active distributors", err)
	}
}

// Guards against pinning a campaign that holds some other chain's or type's
// files, which would mean the campaign was matched wrongly.
func TestUpdateValuesRejectsForeignCycleFile(t *testing.T) {
	mismatched := strings.Replace(values(t), `"type": "LM",
                    "cycles": [`, `"type": "LM",
                    "distributor": "0xNEW",
                    "cycles": [`, 1)

	if _, _, err := updateValues(mismatched, []pair{{ChainID: "8453", RewardType: "LM"}}, 63, testPrefix); err == nil ||
		!strings.Contains(err.Error(), "is not a 8453 LM file") {
		t.Errorf("error = %v, want a complaint about the foreign cycle file", err)
	}
}

func TestUpdateValuesRejectsUnconfiguredChain(t *testing.T) {
	if _, _, err := updateValues(values(t), []pair{{ChainID: "999", RewardType: "EG"}}, 63, testPrefix); err == nil ||
		!strings.Contains(err.Error(), "not configured") {
		t.Errorf("error = %v, want a complaint about the unconfigured chain", err)
	}
}

func TestFindJSONBlockRequiresExactlyOneBlock(t *testing.T) {
	if _, err := findJSONBlock("nothing: here\n"); err == nil ||
		!strings.Contains(err.Error(), "found 0") {
		t.Errorf("error = %v, want a complaint about the missing block", err)
	}
}
