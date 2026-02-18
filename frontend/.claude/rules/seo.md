---
description: Head meta tags and SEO patterns
globs: src/routes/**/*.tsx
---

# Head Meta & SEO

## Meta hierarchy

- Base meta tags in `src/routes/__root.tsx` via `head()` function
- Routes override with `head: () => ({ meta: [...] })`
- Child routes inherit and can extend parent meta
- **No variables in head**: Only literals and `import.meta.env.VITE_*`

## Route implementation

```tsx
head: () => ({
  meta: [
    { title: `Page Name | ${import.meta.env.VITE_BASE_TITLE}` },
    { name: 'description', content: 'Description of this page' },
    { property: 'og:description', content: 'Description of this page' },
    { name: 'twitter:description', content: 'Description of this page' },
  ],
})
```

## Config injection

The Go backend injects `window.__CONFIG__` with runtime configuration (title, baseUrl). This is read by `src/utils/config.ts` and used for API base URL resolution in `src/utils/api-config.ts`.
