package client

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/containerd/containerd/v2/core/content"
	contentlocal "github.com/containerd/containerd/v2/plugins/content/local"
	"github.com/moby/buildkit/client/ociindex"
	digest "github.com/opencontainers/go-digest"
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"
)

func writeBlob(ctx context.Context, t *testing.T, cs content.Store, data []byte) digest.Digest {
	t.Helper()
	dgst := digest.FromBytes(data)
	desc := ocispecs.Descriptor{Digest: dgst, Size: int64(len(data))}
	err := content.WriteBlob(ctx, cs, dgst.String(), bytes.NewReader(data), desc)
	require.NoError(t, err)
	return dgst
}

func listDigests(ctx context.Context, t *testing.T, cs content.Store) map[digest.Digest]struct{} {
	t.Helper()
	result := map[digest.Digest]struct{}{}
	err := cs.Walk(ctx, func(info content.Info) error {
		result[info.Digest] = struct{}{}
		return nil
	})
	require.NoError(t, err)
	return result
}

// setupCacheStore creates a content store with an index.json and a manifest
// referencing the given config and layers. Returns the store path, content
// store, and manifest digest.
func setupCacheStore(ctx context.Context, t *testing.T, configData []byte, layersData [][]byte, tag string) (string, content.Store, digest.Digest) {
	t.Helper()
	dir := t.TempDir()
	cs, err := contentlocal.NewStore(dir)
	require.NoError(t, err)

	configDgst := writeBlob(ctx, t, cs, configData)

	layers := make([]ocispecs.Descriptor, len(layersData))
	for i, ld := range layersData {
		dgst := writeBlob(ctx, t, cs, ld)
		layers[i] = ocispecs.Descriptor{Digest: dgst, Size: int64(len(ld))}
	}

	manifest := ocispecs.Manifest{
		MediaType: ocispecs.MediaTypeImageManifest,
		Config: ocispecs.Descriptor{
			Digest:    configDgst,
			Size:      int64(len(configData)),
			MediaType: "application/vnd.buildkit.cacheconfig.v0",
		},
		Layers: layers,
	}
	manifestData, err := json.Marshal(manifest)
	require.NoError(t, err)
	manifestDgst := writeBlob(ctx, t, cs, manifestData)

	idx := ociindex.NewStoreIndex(dir)
	err = idx.Put(ocispecs.Descriptor{
		Digest:    manifestDgst,
		Size:      int64(len(manifestData)),
		MediaType: ocispecs.MediaTypeImageManifest,
	}, ociindex.Tag(tag))
	require.NoError(t, err)

	return dir, cs, manifestDgst
}

func TestResetCacheStoreImageManifest(t *testing.T) {
	ctx := context.Background()
	dir, cs, manifestDgst := setupCacheStore(ctx, t,
		[]byte(`{"test":"config"}`),
		[][]byte{[]byte("layer1-data")},
		"latest",
	)

	// Write orphan blob
	orphanDgst := writeBlob(ctx, t, cs, []byte("orphan-old-layer"))

	// Verify 4 blobs exist
	require.Len(t, listDigests(ctx, t, cs), 4)

	err := resetCacheStore(ctx, cs, dir)
	require.NoError(t, err)

	remaining := listDigests(ctx, t, cs)
	require.Len(t, remaining, 3) // manifest + config + layer
	require.Contains(t, remaining, manifestDgst)
	require.NotContains(t, remaining, orphanDgst)
}

func TestResetCacheStoreMultipleTags(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cs, err := contentlocal.NewStore(dir)
	require.NoError(t, err)

	// Manifest 1 (tag=v1)
	config1Dgst := writeBlob(ctx, t, cs, []byte(`{"tag":"v1"}`))
	layer1Dgst := writeBlob(ctx, t, cs, []byte("layer1-only-in-v1"))
	m1 := ocispecs.Manifest{
		MediaType: ocispecs.MediaTypeImageManifest,
		Config:    ocispecs.Descriptor{Digest: config1Dgst, Size: 12, MediaType: "application/vnd.buildkit.cacheconfig.v0"},
		Layers:    []ocispecs.Descriptor{{Digest: layer1Dgst, Size: 17}},
	}
	m1Data, err := json.Marshal(m1)
	require.NoError(t, err)
	m1Dgst := writeBlob(ctx, t, cs, m1Data)

	// Manifest 2 (tag=v2)
	config2Dgst := writeBlob(ctx, t, cs, []byte(`{"tag":"v2"}`))
	layer2Dgst := writeBlob(ctx, t, cs, []byte("layer2-only-in-v2"))
	m2 := ocispecs.Manifest{
		MediaType: ocispecs.MediaTypeImageManifest,
		Config:    ocispecs.Descriptor{Digest: config2Dgst, Size: 12, MediaType: "application/vnd.buildkit.cacheconfig.v0"},
		Layers:    []ocispecs.Descriptor{{Digest: layer2Dgst, Size: 17}},
	}
	m2Data, err := json.Marshal(m2)
	require.NoError(t, err)
	m2Dgst := writeBlob(ctx, t, cs, m2Data)

	// Orphan blob
	orphanDgst := writeBlob(ctx, t, cs, []byte("orphan-blob"))

	// Write index.json with both tags
	idx := ociindex.NewStoreIndex(dir)
	require.NoError(t, idx.Put(ocispecs.Descriptor{Digest: m1Dgst, Size: int64(len(m1Data)), MediaType: ocispecs.MediaTypeImageManifest}, ociindex.Tag("v1")))
	require.NoError(t, idx.Put(ocispecs.Descriptor{Digest: m2Dgst, Size: int64(len(m2Data)), MediaType: ocispecs.MediaTypeImageManifest}, ociindex.Tag("v2")))

	require.Len(t, listDigests(ctx, t, cs), 7)

	err = resetCacheStore(ctx, cs, dir)
	require.NoError(t, err)

	remaining := listDigests(ctx, t, cs)
	require.Len(t, remaining, 6)
	require.Contains(t, remaining, m1Dgst)
	require.Contains(t, remaining, config1Dgst)
	require.Contains(t, remaining, layer1Dgst)
	require.Contains(t, remaining, m2Dgst)
	require.Contains(t, remaining, config2Dgst)
	require.Contains(t, remaining, layer2Dgst)
	require.NotContains(t, remaining, orphanDgst)
}

func TestResetCacheStoreNoOrphans(t *testing.T) {
	ctx := context.Background()
	dir, cs, _ := setupCacheStore(ctx, t,
		[]byte(`{"test":"config"}`),
		nil,
		"latest",
	)

	err := resetCacheStore(ctx, cs, dir)
	require.NoError(t, err)

	require.Len(t, listDigests(ctx, t, cs), 2) // manifest + config
}
