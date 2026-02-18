---
description: Theming and color token usage
globs: src/**/*.tsx, src/**/*.ts, src/index.css
---

# Theming

Two-tier color architecture defined in `src/index.css`:

- **Tier 1:** Primitive scales (neutral) with 50-950 shades
- **Tier 2:** Semantic tokens that reference Tier 1

## Semantic tokens

- Brand: `primary`, `secondary`, `accent`
- UI: `background`, `surface`, `foreground`, `muted`, `border`
- State: `success`, `warning`, `danger`
- Domain: `external`, `scheduled`, `incremental`
- Interval: `interval-slot`, `interval-epoch`, `interval-other`
- Edge: `or-edge`

## Usage

Always use semantic tokens — never primitive scales directly (`bg-neutral-500`).

```tsx
className="bg-primary text-foreground border-border"
className="hover:bg-accent text-muted"
className="text-external"    // model type colors
className="text-scheduled"
className="text-incremental"
```

Programmatic access (e.g., for ReactFlow edge styles):

```tsx
style={{ stroke: 'var(--color-incremental)' }}
style={{ fill: 'var(--color-muted)' }}
```

For hooks:

```tsx
import { useThemeColors } from '@/hooks/useThemeColors';
const colors = useThemeColors(); // { primary, background, external, ... }
```

## Modifying theme

Edit semantic mappings in `src/index.css` at `@layer base` (`:root` for light, `html.dark` for dark).

## ESLint enforcement

Custom rules prevent regressions:
- `cbt/no-hardcoded-colors` — bans hex/rgb/hsl in className and style props
- `cbt/no-primitive-color-scales` — bans `neutral-*` in className
