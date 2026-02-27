This is a yarn classic + vite + vanilla ts project

Assume yarn dev is already running on localhost 5173

The full documentation for openmct is located at @OPENMCT.md

# Architecture

## Adding a new data source

1. Create `src/sources/<name>.ts` implementing `DataSource` (`allKeys()` + `subscribe()`).
2. In `src/main.ts`, call `registerDataSource(new MySource())` (guard with `getBackendType()` if needed).

## Adding a new layout

1. Refer to `src/layout/avionics.ts` for an example.
2. In `src/main.ts`, call `openmct.install(MyLayoutPlugin)` under the `// register layouts here` comment.

### Measured timestamp vs. received timestamp

`Datum.timestampMs` is a single timestamp. If a sensor value carries both a
_measured_ timestamp (e.g. GPS fix time, which may be unavailable) and a
_received_ timestamp (wall-clock time the data arrived, always present), model
them as **two separate keys**:

- when measured timestamp is avaliable, emit one key with timestampMs = measured timestamp
- when measured timestamp is unavaliable, emit another key with timestampMs = received timestamp

Then, in the layout, create an overlay plot with both keys.

## Adding a new plugin

1. Create `src/plugins/<name>.ts`. Export a plugin function `MyPlugin(openmct: any)`.
2. In `src/main.ts`, call `openmct.install(MyPlugin)`.

# TypeScript

- **Prefer readability** over performance: dumb code over clever solutions (e.g. for loop > reduce)
- **offensive programming**: aggressively use types to convey contracts
  - assert non-null if all code path supports it (with comment explaining why), instead of adding if else.
- **No `any`**: always use proper types
- **No vague object types**: `Record<string, unknown>`, `object`, etc. are forbidden; define explicit types or derive.
- **Single source of truth**: derive types from generated types or existing interfaces using `Pick`, `Omit` etc; reuse SDK types even if they have extra fields; use `ReturnType`, `Awaited`, `Parameters`, etc when types aren't exported
- **Prefer `Nullish<T>`** from `utils/types` over `T | null | undefined`
- **No barrel files**: import directly from concrete module files, not re-export `index.ts` files
- **Document public APIs**: JSDoc on all public methods/constructors with `@param` / `@returns`; keep docs in sync when modifying
- **Refactor call sites**: when changing a function signature, update all call sites instead of adding optional parameters
- **Avoid hardcoded strings**: export and reuse constants if possible

# Before Finishing

**IMPORTANT**: Before you consider a task complete, **always**: (unless the task is trivial)

1. **Simplify**: Re-examine all the files you just edited, think about all the related existing logic.

- Collapse single-use abstractions
- Align types with actual usage
- Aggressively factor out duplicate logic between new code and existing code
- Any other simplification you see appropriate

2. **Lint**: Run `yarn tsc --noEmit` and ensure no type errors.
3. **Test**: Run `yarn test` and ensure all tests pass
4. **Format**: Run `yarn format` at repo root.
