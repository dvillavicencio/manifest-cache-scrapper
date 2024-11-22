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

const baseUrl = "https://www.bungie.net"
const manifestPath = "/Platform/Destiny2/Manifest"

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
	Mode                      int              `json:"directActivityModeType"` // Using *int to handle nil values
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
    log.Fatalf("Failed to unmarshal JSON while fetching manifest: %v", err)
    return Manifest{}, fmt.Errorf("Failed to unmarshal JSON while fetching manifest: %v", err)
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

/**
* Filter out activities based on the mode
*/
func filterActivities(manifestResponse ManifestResponse) ManifestResponse {
	// Filtering out manifest objects
  log.Printf("Size of response before filtering: %d", len(manifestResponse))
  filteredManifest := make(ManifestResponse)

	for hash, data := range manifestResponse {
		if data.Mode != 4 {
			// Skip if mode is nil or mode is not equal to 4
			continue
		}
		filteredManifest[hash] = data
	}

  log.Printf("Size of response after filtering: %d", len(filteredManifest))
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

/**
* Save the data to Redis
*/
func saveToRedis(ctx context.Context, client *redis.Client, data ManifestResponse) error {
  log.Printf("Saving %d items to Redis", len(data))
  for key, value := range data {

    jsonValue, err := json.Marshal(value)
    log.Printf("Saving hash [%s] with value [%v] to Redis...", key, value)
    if err != nil {
      return fmt.Errorf("Failed to serialize data to JSON for key [%s] and value [%v]. Error: %v", key, value, err)
    }

    client.Set(ctx, key, jsonValue, 0)
  }
  log.Printf("Finished saving all items to Redis!")
  return nil
}

func flattenMaps(responses ...ManifestResponse) ManifestResponse {
    result := make(ManifestResponse)

    for _, m := range responses {
        for key, value := range m {
            result[key] = value // Overwrites if the key already exists
        }
    }

    log.Printf("Size of flattening data is: %d", len(result))
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
    log.Fatalf("Error fetching race entities: %v", err)
    return
  }
  classInfo, err := fetchManifestEntities(baseUrl + manifest.Response.JSONWorldComponentContentPaths.En.DestinyClassDefinition)
  if err != nil {
    log.Fatalf("Error fetching class entities: %v", err)
  }
  genderInfo, err := fetchManifestEntities(baseUrl + manifest.Response.JSONWorldComponentContentPaths.En.DestinyGenderDefinition)
  if err != nil {
    log.Fatalf("Error fetching gender entities: %v", err)
  }

  activityInfo, err := fetchManifestEntities(baseUrl + manifest.Response.JSONWorldComponentContentPaths.En.DestinyActivityDefinition)
  if err != nil {
    log.Fatalf("Error fetching activity entities: %v", err)
  }

  filteredActivities := filterActivities(activityInfo)
  data := flattenMaps(raceInfo, classInfo, genderInfo, filteredActivities) 
  
  saveToRedis(ctx, client, data)
}

