package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"github.com/redis/go-redis/v9"
)

const baseUrl = "https://www.bungie.net/Platform/"
const manifestPath = "Destiny2/Manifest"

type Manifest struct {
	Response struct {
		JSONWorldComponentContentPaths struct {
			En struct {
				DestinyActivityDefinition string `json:"DestinyActivityDefinition"`
				DestinyClassDefinition    string `json:"DestinyClassDefinition"`
				DestinyGenderDefinition   string `json:"DestinyGenderDefinition"`
				DestinyRaceDefinition     string `json:"DestinyRaceDefinition"`
			} `json:"en"`
		} `json:"jsonWorldComponentContentPaths"`
	} `json:"Response"`
}

type ManifestResponse map[string]ManifestObject

type ManifestObject struct {
	Mode                      *int              `json:"mode,omitempty"` // Using *int to handle nil values
	DisplayProperties         DisplayProperties `json:"displayProperties"`
	OriginalDisplayProperties DisplayProperties `json:"originalDisplayProperties"`
	ReleaseIcon               string            `json:"releaseIcon"`
	ReleaseTime               int               `json:"releaseTime"`
	// Other fields omitted for brevity
}

type DisplayProperties struct {
	Description string `json:"description"`
	Name        string `json:"name"`
	Icon        string `json:"icon"`
	HasIcon     bool   `json:"hasIcon"`
}

/**
* Fetches the latest manifest
*/
func fetchManifest(url string) (Manifest, error) {
  resp, err := http.Get(url)
  if err != nil {
    log.Fatalf("Failed to call the Bnet manifest: %v", err)
    return Manifest{}, fmt.Errorf("Failed to call the Bnet manifest: %v", err)
  }

  defer resp.Body.Close()

  body, err := io.ReadAll(resp.Body)
  if err != nil {
    log.Fatalf("Failed to read body from Bnet response: %v", err)
  }

  var manifest Manifest
  if err := json.Unmarshal(body, &manifest); err != nil {
    log.Fatalf("Failed to unmarshal JSON: %v", err)
    return Manifest{}, fmt.Errorf("Failed to unmarshal JSON: %v", err)
  }

  return manifest, nil
}

/**
* Fetches all the corresponding manifest entities
*/
func fetchManifestEntities(url string) (ManifestResponse, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	var manifest ManifestResponse
	if err := json.Unmarshal(body, &manifest); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON: %w", err)
	}
  return manifest, nil
}

func filterActivities(manifestResponse ManifestResponse) ManifestResponse {
	// Filtering out manifest objects
  filteredManifest := make(ManifestResponse)
	for hash, data := range manifestResponse {
		if data.Mode == nil || *data.Mode != 3 {
			// Skip if mode is nil or mode is not equal to 3
			continue
		}
		filteredManifest[hash] = data
	}

	return filteredManifest
}

func clearCache(ctx context.Context, client *redis.Client) error {
  
  result, err := client.FlushAll(ctx).Result();
  if err != nil {
    return fmt.Errorf("Failed to flush the Redis cache: %w", err)
  }
  log.Printf("Redis cache cleared: %s", result)
  return nil;
}

func saveToRedis(ctx context.Context, client *redis.Client, data ManifestResponse) error {
  for key, value := range data {
    if err := client.Set(ctx, key, value, 0); err != nil {
      log.Panicf("Something happened when saving to Redis key [%s] and value [%v]", key, value)
      return fmt.Errorf("Something happened when saving to Redis!")
    }
  } 
  return nil
}

func flattenMaps(responses ...ManifestResponse) ManifestResponse {
    result := make(ManifestResponse)

    for _, m := range responses {
        for key, value := range m {
            result[key] = value // Overwrites if the key already exists
        }
    }

    return result
}

func main() {
  client := redis.NewClient(&redis.Options{
      Addr: "localhost:6379",
      Password: "",
      DB: 0,
      Protocol: 2,
    })
  
  ctx := context.Background()
  if err := clearCache(ctx, client); err != nil {
    fmt.Println("Error clearing cache: ", err)
    return
  } 

  manifest, err := fetchManifest(baseUrl + manifestPath)
  if err != nil {
    log.Fatalf("%v", err)
  } 

  raceInfo, err := fetchManifestEntities(baseUrl + manifest.Response.JSONWorldComponentContentPaths.En.DestinyRaceDefinition)
  if err != nil {
    log.Fatalf("Error fetching race entities")
    return
  }
  classInfo, err := fetchManifestEntities(baseUrl + manifest.Response.JSONWorldComponentContentPaths.En.DestinyClassDefinition)
  if err != nil {
    log.Fatalf("Error fetching class entities")
  }
  genderInfo, err := fetchManifestEntities(baseUrl + manifest.Response.JSONWorldComponentContentPaths.En.DestinyGenderDefinition)
  if err != nil {
    log.Fatalf("Error fetching gender entities")
  }

  activityInfo, err := fetchManifestEntities(baseUrl + manifest.Response.JSONWorldComponentContentPaths.En.DestinyActivityDefinition)
  if err != nil {
    log.Fatalf("Error fetching activity entities")
  }

  filteredActivities := filterActivities(activityInfo)
  data := flattenMaps(raceInfo, classInfo, genderInfo, activityInfo, filteredActivities) 
  
  saveToRedis(ctx, client, data)

}

