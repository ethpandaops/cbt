---
description: Storybook story conventions
globs: src/**/*.stories.tsx
---

# Storybook

## Co-location

Stories are co-located with their components:

```
ComponentName/
  ComponentName.tsx
  ComponentName.stories.tsx
  index.ts
```

## Decorators

Add a background wrapper decorator to new stories:

```tsx
decorators: [
  Story => (
    <div className="bg-background p-8">
      <Story />
    </div>
  ),
],
```

## Story titles

Use the full nested path matching the component category:

```
Components/Domain/Coverage/CoverageBar
Components/Elements/TypeBadge
Components/Forms/ModelSearchCombobox
```

## Theme toggle

Storybook toolbar includes a theme toggle (light/dark) via `@storybook/addon-themes`. Stories are wrapped in `ThemeProvider` automatically by preview decorators.

## MSW

Mock Service Worker is available for API mocking. Configure per-story handlers:

```tsx
parameters: {
  msw: {
    handlers: [
      http.get('/api/v1/endpoint', () => HttpResponse.json({ ... })),
    ],
  },
},
```
