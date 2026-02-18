# CBT Frontend

## Commands

```bash
pnpm dev                    # Dev server proxying to backend
pnpm lint                   # ESLint with custom color rules
pnpm test                   # Run all tests (unit + storybook)
pnpm test:unit              # Vitest unit tests only
pnpm build                  # Production build
pnpm typecheck              # Type check without emitting
pnpm storybook              # Storybook dev server
```

## Libraries

- pnpm v10, node v22, vite v7, react v19, typescript v5
- tailwindcss v4, headlessui v2, heroicons v2
- @tanstack/react-query v5, @tanstack/router-plugin v1
- zod v4, clsx
- storybook v10, vitest v4, msw v2

## Project Structure

```bash
src/
  routes/                                   # Route definitions using TanStack Router
    __root.tsx                              # Root layout with header, providers, navigation
    index.tsx                               # "/" - Dashboard route
    dag.tsx                                 # "/dag" - DAG view route
    model/$id.tsx                           # "/model/:id" - Model detail route

  components/                               # All UI components (categorized)
    Elements/                               # Basic UI building blocks
      TypeBadge/                            # Model type badge
      SQLCodeBlock/                         # SQL syntax highlighting
      BackToDashboardButton/                # Navigation button
    Feedback/                               # User feedback
      ErrorState/                           # Error display
      LoadingState/                         # Loading spinner
      CustomErrorComponent/                 # Router error boundary
    Forms/                                  # Form controls
      ModelSearchCombobox/                  # Global model search
      TransformationSelector/               # Interval type selector
      RangeSlider/                          # Range slider control
    Layout/                                 # Structure and layout
      ThemeToggle/                          # Dark/light mode toggle
    Navigation/                             # Navigation elements
      ZoomControls/                         # Zoom slider for coverage
      ZoomPresets/                          # Zoom preset buttons
    Overlays/                               # Modals and dialogs
      CoverageDebugDialog/                  # Debug coverage details
    Domain/                                 # Domain-specific components
      Coverage/                             # Coverage visualization
        CoverageBar/                        # Horizontal coverage bar
        CoverageTooltip/                    # Hover tooltip for coverage
        ModelCoverageRow/                   # Row in coverage table
        DependencyRow/                      # Dependency coverage row
      DAG/                                  # Directed acyclic graph
        DagGraph/                           # ReactFlow DAG visualization
        DagNode/                            # Custom DAG node types
      Models/                               # Model information
        ModelDetailView/                    # Full model detail page
        ModelHeader/                        # Model page header
        ModelInfoCard/                      # Model info card grid
        ModelSkeleton/                      # Loading skeleton
        IncrementalModelsSection/           # Incremental models dashboard
        ScheduledTransformationsSection/    # Scheduled models dashboard

  providers/                                # React Context Providers
    ThemeProvider/                           # Dark/light theme provider

  contexts/                                 # React Context definitions
    ThemeContext/                            # Theme context

  hooks/                                    # Custom React hooks
    useTheme/                               # Theme toggle hook
    useThemeColors/                         # Imperative color access

  api/                                      # Generated API client (do not edit)
    @tanstack/react-query.gen.ts            # TanStack Query hooks

  types/                                    # TypeScript types
    modelTypes.ts                           # Model type config (external, incremental, scheduled)

  utils/                                    # Utility functions
    api-config.ts                           # API base URL configuration
    config.ts                               # Runtime config (window.__CONFIG__)
    color.ts                                # Color resolution helpers
    interval-transform.ts                   # Interval type transformations
    or-group-colors.ts                      # OR group decorative palette
    schedule-parser.ts                      # Cron schedule parsing

  main.tsx                                  # Application entry point
  index.css                                 # Global styles, theme tokens, animations
  routeTree.gen.ts                          # Generated route tree (auto-generated)
```

## Architecture Patterns

### Component Organization

Each component gets its own directory:

```
ComponentName/
  ComponentName.tsx           # Implementation
  ComponentName.stories.tsx   # Co-located Storybook story
  index.ts                    # Barrel export
```

### Component Categories

- **Elements**: Basic reusable UI pieces
- **Feedback**: Loading, error, empty states
- **Forms**: User input controls
- **Layout**: Structural components
- **Navigation**: Navigation and zoom controls
- **Overlays**: Modals and dialogs
- **Domain**: Business-logic components (Coverage, DAG, Models)

### Color System

Two-tier color architecture in `src/index.css`:
- **Tier 1**: Primitive `neutral` scale (50-950) — only in theme definition
- **Tier 2**: Semantic tokens — used in all components

Semantic tokens: `primary`, `secondary`, `accent`, `background`, `surface`, `foreground`, `muted`, `border`, `success`, `warning`, `danger`

CBT domain tokens: `external`, `scheduled`, `incremental`, `interval-slot`, `interval-epoch`, `interval-other`, `or-edge`

Custom ESLint rules (`cbt/no-hardcoded-colors`, `cbt/no-primitive-color-scales`) enforce token usage.

## Development Guidelines

- Use semantic color tokens — never hardcoded colors
- Use `@/` path alias for imports (not relative)
- Use `@/api/@tanstack/react-query.gen.ts` hooks for all API calls
- Create Storybook stories for all new components
- Use `pnpm storybook` with Playwright MCP for UI iteration
- Write Vitest tests for hooks and utilities
- Run `pnpm lint` and `pnpm build` before committing
- Use Tailwind v4 class naming (`bg-linear-to-*`, `shadow-xs`, etc.)

### Naming Conventions

- **Routes** (`.tsx`): lowercase — `index.tsx`, `dag.tsx`, `model/$id.tsx`
- **Components** (`.tsx`): PascalCase — `DagGraph.tsx`, `ModelInfoCard.tsx`
- **Hooks** (`.ts`): camelCase starting with `use` — `useTheme.ts`
- **Utils** (`.ts`): kebab-case — `api-config.ts`, `schedule-parser.ts`
- **Barrel exports**: `index.ts`

## Additional Rules

Detailed standards in `.claude/rules/`:

- [Loading States](.claude/rules/loading-states.md) — Skeleton/shimmer patterns
- [SEO](.claude/rules/seo.md) — Head meta hierarchy
- [Storybook](.claude/rules/storybook.md) — Decorator pattern, story title convention
- [Theming](.claude/rules/theming.md) — Two-tier color architecture, semantic tokens
