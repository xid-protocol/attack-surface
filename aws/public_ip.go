package aws

import (
	"context"
	"encoding/json"
	"log"
	"sort"
	"strings"

	"github.com/colin-404/logx"
	"github.com/xid-protocol/xidp/protocols"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func GetPublicIP(ec2Info []*protocols.XID) {
	log.Printf("GetPublicIP start, count=%d", len(ec2Info))

	// Build mapping from provided XIDs (avoid re-querying DB here)
	mapping := BuildPublicIPMapFromXIDs(ec2Info)

	// summary
	var totalIPs int
	for _, ips := range mapping {
		totalIPs += len(ips)
	}
	logx.Infof("public IPs summary: instances=%d, ips=%d", len(mapping), totalIPs)

	// always print full mapping
	buf, _ := json.Marshal(mapping)
	logx.Infof("publicIPs: %s", string(buf))

	log.Printf("GetPublicIP done")
}

func collectFromCollection(ctx context.Context, col *mongo.Collection, result map[string]map[string]struct{}) error {
	if col == nil {
		return nil
	}

	filter := bson.M{
		"$or": []bson.M{
			{"info.type": "aws-instanceid"},
			{"info.type": "aws-instanceID"},
		},
	}
	opts := options.Find().SetBatchSize(200)

	cur, err := col.Find(ctx, filter, opts)
	if err != nil {
		return err
	}
	defer cur.Close(ctx)

	for cur.Next(ctx) {
		var doc bson.M
		if err := cur.Decode(&doc); err != nil {
			log.Printf("decode doc error: %v", err)
			continue
		}

		instanceID := getStringFromPath(doc, []string{"info", "id"})
		if instanceID == "" {
			continue
		}
		payloadAny := getFromPath(doc, []string{"payload"})
		payload, _ := payloadAny.(bson.M)
		if payload == nil {
			// sometimes payload could be map[string]interface{}
			if mm, ok := payloadAny.(map[string]interface{}); ok {
				payload = bson.M(mm)
			}
		}
		if payload == nil {
			continue
		}

		ips := extractPublicIPs(payload)
		if len(ips) == 0 {
			continue
		}
		if _, ok := result[instanceID]; !ok {
			result[instanceID] = map[string]struct{}{}
		}
		for _, ip := range ips {
			if ip == "" {
				continue
			}
			result[instanceID][ip] = struct{}{}
		}
	}
	return cur.Err()
}

// BuildPublicIPMapFromCollection returns map[instanceID][]uniqueSortedPublicIPs
func BuildPublicIPMapFromCollection(ctx context.Context, col *mongo.Collection) (map[string][]string, error) {
	setMap := map[string]map[string]struct{}{}
	if err := collectFromCollection(ctx, col, setMap); err != nil {
		return nil, err
	}
	out := make(map[string][]string, len(setMap))
	for id, set := range setMap {
		ips := make([]string, 0, len(set))
		for ip := range set {
			ips = append(ips, ip)
		}
		sort.Strings(ips)
		out[id] = ips
	}
	return out, nil
}

// BuildPublicIPMapFromXIDs returns instanceID -> []publicIPs from a list of XIDs
func BuildPublicIPMapFromXIDs(items []*protocols.XID) map[string][]string {
	setMap := map[string]map[string]struct{}{}
	for _, record := range items {
		if record == nil {
			continue
		}
		instanceID := ""
		if record.Info != nil && record.Info.ID != "" {
			instanceID = record.Info.ID
		}

		var payloadMap bson.M
		switch v := record.Payload.(type) {
		case bson.M:
			payloadMap = v
		case map[string]interface{}:
			payloadMap = bson.M(v)
		case bson.D:
			payloadMap = bson.M(dToMap(v))
		case bson.Raw:
			payloadMap = toBsonMap(v)
		case []interface{}:
			// Key/Value list representation
			ips := extractPublicIPsFromKV(v)
			if instanceID == "" {
				instanceID = asString(kvFind(v, "instanceid"))
			}
			if instanceID != "" && len(ips) > 0 {
				if _, ok := setMap[instanceID]; !ok {
					setMap[instanceID] = map[string]struct{}{}
				}
				for _, ip := range ips {
					if ip != "" {
						setMap[instanceID][ip] = struct{}{}
					}
				}
			}
			continue
		default:
			payloadMap = toBsonMap(v)
		}

		if payloadMap == nil {
			continue
		}
		if instanceID == "" {
			if s, _ := getAnyCase(map[string]interface{}(payloadMap), "instanceid").(string); s != "" {
				instanceID = s
			}
		}
		ips := extractPublicIPs(payloadMap)
		if instanceID == "" || len(ips) == 0 {
			continue
		}
		if _, ok := setMap[instanceID]; !ok {
			setMap[instanceID] = map[string]struct{}{}
		}
		for _, ip := range ips {
			if ip != "" {
				setMap[instanceID][ip] = struct{}{}
			}
		}
	}

	out := make(map[string][]string, len(setMap))
	for id, set := range setMap {
		ips := make([]string, 0, len(set))
		for ip := range set {
			ips = append(ips, ip)
		}
		sort.Strings(ips)
		out[id] = ips
	}
	return out
}

func extractPublicIPs(payload bson.M) []string {
	set := map[string]struct{}{}

	// Top-level Public IP
	if v := getAnyCase(payload, "publicipaddress"); v != nil {
		if s, ok := v.(string); ok && s != "" {
			set[s] = struct{}{}
		}
	}

	// network interfaces
	if v := getAnyCase(payload, "networkinterfaces"); v != nil {
		if arr, ok := toSlice(v); ok {
			for _, item := range arr {
				m, ok := toMap(item)
				if !ok {
					continue
				}
				// association.publicip
				if assocAny := getAnyCase(m, "association"); assocAny != nil {
					if assoc, ok := toMap(assocAny); ok {
						if p := getAnyCase(assoc, "publicip"); p != nil {
							if s, ok := p.(string); ok && s != "" {
								set[s] = struct{}{}
							}
						}
					}
				}
				// privateipaddresses[].association.publicip
				if pia := getAnyCase(m, "privateipaddresses"); pia != nil {
					if parr, ok := toSlice(pia); ok {
						for _, pi := range parr {
							pm, ok := toMap(pi)
							if !ok {
								continue
							}
							if assocAny := getAnyCase(pm, "association"); assocAny != nil {
								if assoc, ok := toMap(assocAny); ok {
									if p := getAnyCase(assoc, "publicip"); p != nil {
										if s, ok := p.(string); ok && s != "" {
											set[s] = struct{}{}
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}

	// convert set to slice
	var out []string
	for ip := range set {
		out = append(out, ip)
	}
	return out
}

// extractPublicIPsFromKV extracts public IPs from a payload stored as a list of {Key,Value} entries.
func extractPublicIPsFromKV(kvs []interface{}) []string {
	set := map[string]struct{}{}

	// Top-level publicipaddress
	if v := kvFind(kvs, "publicipaddress"); v != nil {
		if s := asString(v); s != "" {
			set[s] = struct{}{}
		}
	}

	// Traverse networkinterfaces[].(association.publicip | privateipaddresses[].association.publicip)
	if niAny := kvFind(kvs, "networkinterfaces"); niAny != nil {
		if nis, ok := niAny.([]interface{}); ok {
			for _, nicAny := range nis {
				nicKVs, ok := nicAny.([]interface{})
				if !ok {
					continue
				}
				if assocAny := kvFind(nicKVs, "association"); assocAny != nil {
					if assocKVs, ok := assocAny.([]interface{}); ok {
						if p := kvFind(assocKVs, "publicip"); p != nil {
							if s := asString(p); s != "" {
								set[s] = struct{}{}
							}
						}
					}
				}
				if piasAny := kvFind(nicKVs, "privateipaddresses"); piasAny != nil {
					if pias, ok := piasAny.([]interface{}); ok {
						for _, pia := range pias {
							piKVs, ok := pia.([]interface{})
							if !ok {
								continue
							}
							if assocAny := kvFind(piKVs, "association"); assocAny != nil {
								if assocKVs, ok := assocAny.([]interface{}); ok {
									if p := kvFind(assocKVs, "publicip"); p != nil {
										if s := asString(p); s != "" {
											set[s] = struct{}{}
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}

	var out []string
	for ip := range set {
		out = append(out, ip)
	}
	return out
}

// kvFind scans a list of {Key,Value} maps for the given key (case-insensitive) and returns the Value.
func kvFind(list []interface{}, key string) interface{} {
	lower := strings.ToLower(key)
	for _, item := range list {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		var k string
		if v, ok := m["Key"]; ok {
			if s, ok := v.(string); ok {
				k = s
			}
		} else if v, ok := m["key"]; ok {
			if s, ok := v.(string); ok {
				k = s
			}
		}
		if strings.ToLower(k) == lower {
			if v, ok := m["Value"]; ok {
				return v
			}
			if v, ok := m["value"]; ok {
				return v
			}
			return nil
		}
	}
	return nil
}

func asString(v interface{}) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// toBsonMap attempts to convert arbitrary structs (like ec2.Instance) to bson.M using BSON first,
// falling back to JSON via the standard library if necessary.
func toBsonMap(v interface{}) bson.M {
	if v == nil {
		return nil
	}
	if b, err := bson.Marshal(v); err == nil {
		var m bson.M
		if err := bson.Unmarshal(b, &m); err == nil {
			return m
		}
	}
	return nil
}

func toMap(v interface{}) (map[string]interface{}, bool) {
	switch t := v.(type) {
	case bson.M:
		return map[string]interface{}(t), true
	case bson.D:
		return dToMap(t), true
	case map[string]interface{}:
		return t, true
	default:
		return nil, false
	}
}

// dToMap converts bson.D (ordered document) to a plain map
func dToMap(d bson.D) map[string]interface{} {
	m := make(map[string]interface{}, len(d))
	for _, e := range d {
		m[e.Key] = e.Value
	}
	return m
}

// toSlice coerces possible BSON array representations into a generic []interface{}
func toSlice(v interface{}) ([]interface{}, bool) {
	switch t := v.(type) {
	case []interface{}:
		return t, true
	case bson.A:
		return []interface{}(t), true
	default:
		return nil, false
	}
}

func getAnyCase(m map[string]interface{}, key string) interface{} {
	lower := strings.ToLower(key)
	for k, v := range m {
		if strings.ToLower(k) == lower {
			return v
		}
	}
	// Support AWS Go SDK struct JSON tags like PublicIpAddress / PublicIp
	// Try common alternates when querying for instanceId/publicIp keys
	if lower == "instanceid" {
		if v, ok := m["InstanceId"]; ok {
			return v
		}
	}
	if lower == "publicipaddress" {
		if v, ok := m["PublicIpAddress"]; ok {
			return v
		}
	}
	if lower == "networkinterfaces" {
		if v, ok := m["NetworkInterfaces"]; ok {
			return v
		}
	}
	if lower == "association" {
		if v, ok := m["Association"]; ok {
			return v
		}
	}
	if lower == "privateipaddresses" {
		if v, ok := m["PrivateIpAddresses"]; ok {
			return v
		}
	}
	if lower == "publicip" {
		if v, ok := m["PublicIp"]; ok {
			return v
		}
	}
	return nil
}

func getFromPath(m map[string]interface{}, path []string) interface{} {
	cur := m
	for i, key := range path {
		if i == len(path)-1 {
			return getAnyCase(cur, key)
		}
		next := getAnyCase(cur, key)
		nm, ok := toMap(next)
		if !ok {
			return nil
		}
		cur = nm
	}
	return nil
}

func getStringFromPath(m map[string]interface{}, path []string) string {
	if v := getFromPath(m, path); v != nil {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}
