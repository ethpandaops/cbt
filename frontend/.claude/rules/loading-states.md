---
description: Loading state and skeleton patterns
globs: src/components/**/*.tsx, src/routes/**/*.tsx
---

# Loading States

## Skeleton pattern

- Create component-specific skeletons alongside the main component (e.g., `IncrementalModelsSectionSkeleton`)
- Match skeleton layout to actual content structure
- Use `animate-pulse` on placeholder elements
- Use semantic surface colors for skeleton blocks: `bg-surface`, `bg-secondary`

## Existing skeletons

- `ModelSkeleton` — full model detail page skeleton
- `IncrementalModelsSectionSkeleton` — incremental models dashboard
- `ScheduledTransformationsSectionSkeleton` — scheduled models dashboard
- `CoverageDebugDialogSkeleton` — debug dialog content

## Error states

- Use `ErrorState` component from `@/components/Feedback/ErrorState`
- Always consider error states alongside loading states
- Provide actionable error messages where possible

## Loading spinner

- Use `LoadingState` from `@/components/Feedback/LoadingState` for simple loading indicators
