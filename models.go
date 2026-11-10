package main

// The bridge names a model by its model-store registry id (claude-opus-5-5,
// gpt-6-astra), and effort separately. Devin names one model variant per
// effort level (claude-opus-5-5-high). Devin's catalog — `devin models list
// --format json` — is the source of truth for which variants exist, and the
// session's own "model" config option is the source of truth for which of them
// this account may use.

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"sort"
	"strings"
)

type devinModelFamily struct {
	FamilyUID string   `json:"family_uid"`
	Slug      string   `json:"slug"`
	Aliases   []string `json:"aliases"`
	Variants  []struct {
		ModelUID string `json:"model_uid"`
		Label    string `json:"label"`
		CostTier string `json:"cost_tier"`
	} `json:"variants"`
}

func loadDevinModelCatalog(devinPath string) ([]devinModelFamily, error) {
	out, err := exec.Command(devinPath, "models", "list", "--format", "json").Output()
	if err != nil {
		return nil, fmt.Errorf("devin models list: %w", err)
	}
	var listing struct {
		Families []devinModelFamily `json:"families"`
	}
	if err := json.Unmarshal(out, &listing); err != nil {
		return nil, fmt.Errorf("decode devin models list: %w", err)
	}
	if len(listing.Families) == 0 {
		return nil, fmt.Errorf("devin models list returned no model families")
	}
	return listing.Families, nil
}

// resolveDevinModel picks the devin variant for a bridge model and effort.
//
//   - A model that is already a variant uid ("swe-2-flash-medium") is used as
//     it is; effort must then be empty or agree with it.
//   - A family ("claude-opus-5-5") with an effort becomes "<family>-<effort>",
//     which must exist in the catalog.
//   - A family with no effort becomes the family's first variant, which is
//     the one devin's catalog lists first and its own picker opens on.
//
// Anything else is an error naming what devin does offer.
func resolveDevinModel(catalog []devinModelFamily, model, effort string) (string, error) {
	if model == "" {
		return "", nil
	}
	for _, family := range catalog {
		for _, variant := range family.Variants {
			if variant.ModelUID == model {
				if effort != "" && !strings.HasSuffix(model, "-"+effort) && !strings.Contains(model, "-"+effort+"-") {
					return "", fmt.Errorf("model %q is a fixed devin variant and does not match effort %q", model, effort)
				}
				return model, nil
			}
		}
	}
	for _, family := range catalog {
		if family.FamilyUID != model && family.Slug != model {
			continue
		}
		if len(family.Variants) == 0 {
			return "", fmt.Errorf("devin lists model family %q with no variants", model)
		}
		if effort == "" {
			return family.Variants[0].ModelUID, nil
		}
		want := family.FamilyUID + "-" + effort
		for _, variant := range family.Variants {
			if variant.ModelUID == want {
				return want, nil
			}
		}
		return "", fmt.Errorf("devin has no %q: model %q offers efforts %v", want, model, familyEfforts(family))
	}
	return "", fmt.Errorf("devin's model catalog has no %q (families: %v)", model, catalogFamilies(catalog))
}

func familyEfforts(family devinModelFamily) []string {
	var efforts []string
	for _, variant := range family.Variants {
		efforts = append(efforts, strings.TrimPrefix(variant.ModelUID, family.FamilyUID+"-"))
	}
	return efforts
}

func catalogFamilies(catalog []devinModelFamily) []string {
	names := make([]string, 0, len(catalog))
	for _, family := range catalog {
		names = append(names, family.FamilyUID)
	}
	sort.Strings(names)
	return names
}

// effortOfVariant reads the effort back out of a variant uid, for
// session_info: "claude-opus-5-5-high" in family claude-opus-5-5 is "high".
func effortOfVariant(catalog []devinModelFamily, uid string) string {
	for _, family := range catalog {
		for _, variant := range family.Variants {
			if variant.ModelUID == uid {
				return strings.TrimPrefix(uid, family.FamilyUID+"-")
			}
		}
	}
	return ""
}

// familyOfVariant is the bridge-facing model id of a variant.
func familyOfVariant(catalog []devinModelFamily, uid string) string {
	for _, family := range catalog {
		for _, variant := range family.Variants {
			if variant.ModelUID == uid {
				return family.FamilyUID
			}
		}
	}
	return uid
}
