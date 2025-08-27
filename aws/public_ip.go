package aws

// import (
// 	"context"
// 	"encoding/json"
// 	"fmt"
// 	"log"
// 	"os"
// 	"strings"
// 	"time"

// 	"go.mongodb.org/mongo-driver/bson"
// 	"go.mongodb.org/mongo-driver/mongo"
// 	"go.mongodb.org/mongo-driver/mongo/options"
// )

// func GetPublicIP() {
// 	mongoURI := getenvDefault("MONGO_URI", "mongodb://localhost:27017")
// 	mongoDB := getenvDefault("MONGO_DB", "test")

// 	if err := imdbh.InitMongoDB(mongoDB, mongoURI); err != nil {
// 		log.Printf("failed to init mongodb: %v", err)
// 		fmt.Println("{}")
// 		return
// 	}
// 	defer func() {
// 		_ = imdbh.CloseMongoDB()
// 	}()

// 	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
// 	defer cancel()

// 	// Aggregate from both collections, if they exist
// 	instanceToIPs := map[string]map[string]struct{}{}

// 	for _, col := range []string{"aws_info", "xid_info"} {
// 		coll := safeGetCollection(imdbh.GetCollection(col))
// 		if coll == nil {
// 			continue
// 		}
// 		if err := collectFromCollection(ctx, coll, instanceToIPs); err != nil {
// 			log.Printf("warn: collect from %s failed: %v", col, err)
// 		}
// 	}

// 	// Convert set to list
// 	out := map[string][]string{}
// 	for instanceID, ips := range instanceToIPs {
// 		for ip := range ips {
// 			out[instanceID] = append(out[instanceID], ip)
// 		}
// 	}

// 	b, err := json.MarshalIndent(out, "", "  ")
// 	if err != nil {
// 		log.Fatalf("failed to marshal result: %v", err)
// 	}
// 	fmt.Println(string(b))
// }

// func getenvDefault(key, def string) string {
// 	v := os.Getenv(key)
// 	if v == "" {
// 		return def
// 	}
// 	return v
// }

// func safeGetCollection(col *mongo.Collection) *mongo.Collection {
// 	return col
// }

// func collectFromCollection(ctx context.Context, col *mongo.Collection, result map[string]map[string]struct{}) error {
// 	if col == nil {
// 		return nil
// 	}

// 	filter := bson.M{
// 		"$or": []bson.M{
// 			{"info.type": "aws-instanceid"},
// 			{"info.type": "aws-instanceID"},
// 		},
// 	}
// 	opts := options.Find().SetBatchSize(200)

// 	cur, err := col.Find(ctx, filter, opts)
// 	if err != nil {
// 		return err
// 	}
// 	defer cur.Close(ctx)

// 	for cur.Next(ctx) {
// 		var doc bson.M
// 		if err := cur.Decode(&doc); err != nil {
// 			log.Printf("decode doc error: %v", err)
// 			continue
// 		}

// 		instanceID := getStringFromPath(doc, []string{"info", "id"})
// 		if instanceID == "" {
// 			continue
// 		}
// 		payloadAny := getFromPath(doc, []string{"payload"})
// 		payload, _ := payloadAny.(bson.M)
// 		if payload == nil {
// 			// sometimes payload could be map[string]interface{}
// 			if mm, ok := payloadAny.(map[string]interface{}); ok {
// 				payload = bson.M(mm)
// 			}
// 		}
// 		if payload == nil {
// 			continue
// 		}

// 		ips := extractPublicIPs(payload)
// 		if len(ips) == 0 {
// 			continue
// 		}
// 		if _, ok := result[instanceID]; !ok {
// 			result[instanceID] = map[string]struct{}{}
// 		}
// 		for _, ip := range ips {
// 			if ip == "" {
// 				continue
// 			}
// 			result[instanceID][ip] = struct{}{}
// 		}
// 	}
// 	return cur.Err()
// }

// func extractPublicIPs(payload bson.M) []string {
// 	set := map[string]struct{}{}

// 	// Top-level Public IP
// 	if v := getAnyCase(payload, "publicipaddress"); v != nil {
// 		if s, ok := v.(string); ok && s != "" {
// 			set[s] = struct{}{}
// 		}
// 	}

// 	// network interfaces
// 	if v := getAnyCase(payload, "networkinterfaces"); v != nil {
// 		if arr, ok := v.([]interface{}); ok {
// 			for _, item := range arr {
// 				m, ok := toMap(item)
// 				if !ok {
// 					continue
// 				}
// 				// association.publicip
// 				if assocAny := getAnyCase(m, "association"); assocAny != nil {
// 					if assoc, ok := toMap(assocAny); ok {
// 						if p := getAnyCase(assoc, "publicip"); p != nil {
// 							if s, ok := p.(string); ok && s != "" {
// 								set[s] = struct{}{}
// 							}
// 						}
// 					}
// 				}
// 				// privateipaddresses[].association.publicip
// 				if pia := getAnyCase(m, "privateipaddresses"); pia != nil {
// 					if parr, ok := pia.([]interface{}); ok {
// 						for _, pi := range parr {
// 							pm, ok := toMap(pi)
// 							if !ok {
// 								continue
// 							}
// 							if assocAny := getAnyCase(pm, "association"); assocAny != nil {
// 								if assoc, ok := toMap(assocAny); ok {
// 									if p := getAnyCase(assoc, "publicip"); p != nil {
// 										if s, ok := p.(string); ok && s != "" {
// 											set[s] = struct{}{}
// 										}
// 									}
// 								}
// 							}
// 						}
// 					}
// 				}
// 			}
// 		}
// 	}

// 	// convert set to slice
// 	var out []string
// 	for ip := range set {
// 		out = append(out, ip)
// 	}
// 	return out
// }

// func toMap(v interface{}) (map[string]interface{}, bool) {
// 	switch t := v.(type) {
// 	case bson.M:
// 		return map[string]interface{}(t), true
// 	case map[string]interface{}:
// 		return t, true
// 	default:
// 		return nil, false
// 	}
// }

// func getAnyCase(m map[string]interface{}, key string) interface{} {
// 	lower := strings.ToLower(key)
// 	for k, v := range m {
// 		if strings.ToLower(k) == lower {
// 			return v
// 		}
// 	}
// 	return nil
// }

// func getFromPath(m map[string]interface{}, path []string) interface{} {
// 	cur := m
// 	for i, key := range path {
// 		if i == len(path)-1 {
// 			return getAnyCase(cur, key)
// 		}
// 		next := getAnyCase(cur, key)
// 		nm, ok := toMap(next)
// 		if !ok {
// 			return nil
// 		}
// 		cur = nm
// 	}
// 	return nil
// }

// func getStringFromPath(m map[string]interface{}, path []string) string {
// 	if v := getFromPath(m, path); v != nil {
// 		if s, ok := v.(string); ok {
// 			return s
// 		}
// 	}
// 	return ""
// }
