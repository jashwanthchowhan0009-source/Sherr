# White Tiger — 3D assets

Drop your model here as:

```
assets/white_tiger.glb
```

The page (`../index.html`) loads exactly that path via Three.js `GLTFLoader`.

- **Format:** `.glb` (binary glTF). `.gltf` + textures also works if you keep the
  relative paths intact, but a single `.glb` is simplest.
- **Draco compression** is supported out of the box (the decoder is loaded from the
  three.js CDN), so a Draco-compressed export will load fine.
- The model is **auto-centered and auto-scaled** at runtime — you don't need to
  match any particular size or origin. A clean, upright, forward-facing export
  works best (the hero pose then turns it slightly away).
- Prefer a **PBR / MeshStandard**-style material with a white/leucistic fur albedo;
  the studio lighting + image-based reflections are tuned for that.

### Until you add the model
If `white_tiger.glb` is missing, the page shows a tasteful **placeholder**
(the tiger motif on crossed billboard planes inside the neon halo) plus a small
note. Everything else — the glowing ring, lighting, bloom, scroll-driven 360°
rotation and text choreography — works identically, so you can build and preview
the whole experience before the model is ready.

### Where to find a model
Any rigged/*static* white tiger `.glb` works — e.g. exported from Blender, or a
CC-licensed model from Sketchfab (download → glTF/GLB). Just rename it to
`white_tiger.glb` and place it in this folder.
