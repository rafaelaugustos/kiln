package dashboard

import (
	"bytes"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"io/fs"
	"net/http"
	"path"
	"strings"
)

//go:embed assets
var assetFS embed.FS

type asset struct {
	body []byte
	typ  string
}

var assets, files = hashAssets()

var types = map[string]string{
	".css":   "text/css; charset=utf-8",
	".js":    "text/javascript; charset=utf-8",
	".svg":   "image/svg+xml",
	".woff2": "font/woff2",
}

func hashAssets() (map[string]string, map[string]asset) {
	names := make(map[string]string)
	byHash := make(map[string]asset)
	entries, _ := fs.ReadDir(assetFS, "assets")
	for _, css := range []bool{false, true} {
		for _, e := range entries {
			ext := path.Ext(e.Name())
			if (ext == ".css") != css {
				continue
			}
			b, _ := assetFS.ReadFile("assets/" + e.Name())
			if css {
				for from, to := range names {
					b = bytes.ReplaceAll(b, []byte("url("+from+")"), []byte("url("+to+")"))
				}
			}
			sum := sha256.Sum256(b)
			hashed := strings.TrimSuffix(e.Name(), ext) + "." + hex.EncodeToString(sum[:5]) + ext
			names[e.Name()] = hashed
			byHash[hashed] = asset{body: b, typ: types[ext]}
		}
	}
	return names, byHash
}

func serveAsset(w http.ResponseWriter, r *http.Request) {
	a, ok := files[r.PathValue("name")]
	if !ok {
		http.NotFound(w, r)
		return
	}
	h := w.Header()
	h.Set("Content-Type", a.typ)
	h.Set("Cache-Control", "public, max-age=31536000, immutable")
	w.Write(a.body)
}
