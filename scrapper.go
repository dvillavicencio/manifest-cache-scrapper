package scrapper

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
* Fetches and filters all the corresponding manifest entities
*/
func fetchAndFilterManifestEntities(url string) (ManifestResponse, error) {
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

	// Create a new map for filtered results
	filteredManifest := make(ManifestResponse)

	// Filtering out manifest objects
	for hash, data := range manifest {
		if data.Mode == nil || *data.Mode != 3 {
			// Skip if mode is nil or mode is not equal to 3
			continue
		}
		filteredManifest[hash] = data
	}

	return filteredManifest, nil
}

func clearCache() {
  client := redis.NewClient(&redis.Options{
    Addr: "localhost:6379",
    Password: "",
    DB: 0,
    Protocol: 2,
  })
  ctx := context.Background();

  client.FlushAll(ctx);
}

func saveToRedis(client Redis, data ManifestResponse) {
}

func main() {
  clearCache()
  manifest, err := fetchManifest(baseUrl + manifestPath)
  if err != nil {
    log.Fatalf("%v", err)
  }

	filteredManifest, err := fetchAndFilterManifestEntities(baseUrl + ) 
	if err != nil {
		log.Fatalf("Error fetching and filtering manifest: %v", err)
	}

  saveToRedis(filteredManifest) 
}

