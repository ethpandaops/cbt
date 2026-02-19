import { type JSX, useEffect, useMemo, useState } from 'react';
import { Dialog, DialogPanel, DialogTitle, DialogBackdrop } from '@headlessui/react';
import { AdjustmentsHorizontalIcon } from '@heroicons/react/24/outline';
import CodeMirror from '@uiw/react-codemirror';
import { json as jsonLanguage, jsonParseLinter } from '@codemirror/lang-json';
import { linter } from '@codemirror/lint';
import { EditorView } from '@codemirror/view';

interface ConfigOverride {
  model_id: string;
  model_type: string;
  enabled?: boolean;
  override: Record<string, unknown> | null;
  updated_at: string;
}

interface ConfigOverrideDialogProps {
  open: boolean;
  onClose: () => void;
  onSave: (config: string) => void;
  onDelete: () => void;
  modelId: string;
  modelType: 'incremental' | 'scheduled' | 'external';
  currentOverride?: ConfigOverride | null;
  baseConfig?: Record<string, unknown> | null;
  isSaving?: boolean;
  isDeleting?: boolean;
}

type ValidationState = 'idle' | 'valid' | 'invalid';

export function ConfigOverrideDialog({
  open,
  onClose,
  onSave,
  onDelete,
  modelId,
  modelType,
  currentOverride,
  baseConfig,
  isSaving,
  isDeleting,
}: ConfigOverrideDialogProps): JSX.Element {
  const [json, setJson] = useState('');
  const [parseError, setParseError] = useState<string | null>(null);
  const [validationState, setValidationState] = useState<ValidationState>('idle');
  const [hasInitializedForOpen, setHasInitializedForOpen] = useState(false);
  const [initialFingerprint, setInitialFingerprint] = useState<string | null>(null);
  const [hasRemoteChangeWhileOpen, setHasRemoteChangeWhileOpen] = useState(false);

  const busy = isSaving || isDeleting;
  const initialJson = useMemo(() => {
    if (currentOverride) {
      // Build a combined object from the current override
      const combined: Record<string, unknown> = {};

      if (currentOverride.enabled != null) {
        combined.enabled = currentOverride.enabled;
      }

      if (currentOverride.override) {
        combined.config = currentOverride.override;
      }

      return JSON.stringify(combined, null, 2);
    }

    if (baseConfig) {
      // Pre-populate with the original base config from the backend
      return JSON.stringify({ config: baseConfig }, null, 2);
    }

    return '{}';
  }, [currentOverride, baseConfig]);
  const sourceFingerprint = useMemo(
    () =>
      JSON.stringify({
        modelType,
        baseConfig: baseConfig ?? null,
        enabled: currentOverride?.enabled ?? null,
        override: currentOverride?.override ?? null,
        updatedAt: currentOverride?.updated_at ?? null,
      }),
    [modelType, baseConfig, currentOverride]
  );
  const editorExtensions = useMemo(() => [jsonLanguage(), linter(jsonParseLinter())], []);
  const editorTheme = useMemo(
    () =>
      EditorView.theme({
        '&': {
          backgroundColor: 'var(--background-value)',
          color: 'var(--foreground-value)',
          fontSize: '12px',
        },
        '&.cm-focused': {
          outline: 'none',
        },
        '.cm-content': {
          fontFamily: 'ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, Liberation Mono, Courier New, monospace',
          padding: '0.75rem 0',
        },
        '.cm-line': {
          padding: '0 0.75rem',
        },
        '.cm-gutters': {
          backgroundColor: 'var(--background-value)',
          color: 'var(--muted-value)',
          border: 'none',
        },
        '.cm-activeLine': {
          backgroundColor: 'rgba(46, 94, 255, 0.12)',
        },
        '.cm-activeLineGutter': {
          backgroundColor: 'transparent',
        },
        '.cm-cursor': {
          borderLeftColor: 'var(--accent-value)',
        },
      }),
    []
  );

  const validateJson = (): boolean => {
    try {
      JSON.parse(json);
      setParseError(null);
      setValidationState('valid');
      return true;
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Invalid JSON';
      setParseError(`Invalid JSON: ${errorMessage}`);
      setValidationState('invalid');
      return false;
    }
  };

  useEffect(() => {
    if (!open) {
      setHasInitializedForOpen(false);
      setInitialFingerprint(null);
      setHasRemoteChangeWhileOpen(false);
      return;
    }

    if (!hasInitializedForOpen) {
      setJson(initialJson);
      setInitialFingerprint(sourceFingerprint);
      setHasInitializedForOpen(true);
      setHasRemoteChangeWhileOpen(false);
      setParseError(null);
      setValidationState('idle');
      return;
    }

    if (initialFingerprint != null && sourceFingerprint !== initialFingerprint) {
      setHasRemoteChangeWhileOpen(true);
    }
  }, [open, hasInitializedForOpen, initialJson, sourceFingerprint, initialFingerprint]);

  const handleValidate = (): void => {
    if (busy) return;
    validateJson();
  };

  const handleFormat = (): void => {
    if (busy) return;

    try {
      const formatted = JSON.stringify(JSON.parse(json), null, 2);
      setJson(formatted);
      setParseError(null);
      setValidationState('valid');
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Invalid JSON';
      setParseError(`Invalid JSON: ${errorMessage}`);
      setValidationState('invalid');
    }
  };

  const handleSave = (): void => {
    if (busy) return;

    if (!validateJson()) return;

    onSave(json);
  };

  return (
    <Dialog open={open} onClose={onClose} className="relative z-50">
      <DialogBackdrop className="fixed inset-0 bg-black/40 backdrop-blur-xs" />
      <div className="fixed inset-0 flex items-center justify-center p-4">
        <DialogPanel className="w-full max-w-md rounded-xl border border-border/50 bg-surface p-6 shadow-lg">
          <div className="flex items-center gap-3">
            <div className="flex size-10 items-center justify-center rounded-lg bg-accent/10">
              <AdjustmentsHorizontalIcon className="size-5 text-accent" />
            </div>
            <div>
              <DialogTitle className="text-lg font-semibold text-foreground">Config Override</DialogTitle>
              <p className="text-xs text-muted">{modelId}</p>
            </div>
          </div>

          {currentOverride && (
            <p className="mt-2 text-xs text-muted">
              Last updated: {new Date(currentOverride.updated_at).toLocaleString()}
            </p>
          )}

          <div className="mt-4 space-y-3">
            <div className="flex flex-wrap items-center justify-between gap-2">
              <p className="text-sm font-medium text-muted">Override JSON</p>
              <div className="flex items-center gap-2">
                <button
                  type="button"
                  onClick={handleFormat}
                  disabled={busy}
                  className="rounded-md border border-border/60 bg-background px-2.5 py-1 text-xs font-medium text-muted transition-colors hover:bg-secondary/35 hover:text-foreground disabled:opacity-50"
                >
                  Format JSON
                </button>
                <button
                  type="button"
                  onClick={handleValidate}
                  disabled={busy}
                  className="rounded-md border border-border/60 bg-background px-2.5 py-1 text-xs font-medium text-muted transition-colors hover:bg-secondary/35 hover:text-foreground disabled:opacity-50"
                >
                  Validate
                </button>
              </div>
            </div>
            <div
              className={`overflow-hidden rounded-lg border ${
                parseError ? 'border-danger/55 dark:border-danger/30' : 'border-border/60'
              } bg-background ${busy ? 'opacity-60' : ''}`}
            >
              <CodeMirror
                value={json}
                onChange={value => {
                  setJson(value);
                  setParseError(null);
                  setValidationState('idle');
                }}
                editable={!busy}
                aria-label="Override JSON"
                spellCheck={false}
                basicSetup={{
                  foldGutter: false,
                  highlightActiveLineGutter: true,
                }}
                height="22rem"
                extensions={editorExtensions}
                theme={editorTheme}
              />
            </div>
            {validationState === 'valid' && !parseError && (
              <p className="text-xs font-medium text-success">JSON is valid.</p>
            )}
            {parseError && (
              <p className="rounded-md border border-danger/30 bg-danger/10 px-2.5 py-1.5 text-xs font-medium text-danger dark:border-danger/25 dark:bg-danger/15 dark:text-red-300">
                {parseError}
              </p>
            )}
            {hasRemoteChangeWhileOpen && (
              <p className="text-xs font-medium text-warning">
                Server config changed in the background. Close and reopen to load the latest values.
              </p>
            )}
            <p className="text-[11px] text-muted">
              {modelType === 'external'
                ? 'Overridable: lag, cache intervals.'
                : 'Overridable: enabled, interval, schedules, limits, tags.'}{' '}
              Use <code className="rounded-xs bg-secondary/60 px-1">null</code> to keep existing values.
            </p>
          </div>

          <div className="mt-5 flex items-center justify-between">
            <button
              type="button"
              onClick={onDelete}
              disabled={busy || !currentOverride}
              className="rounded-md border border-danger/35 bg-danger/10 px-2.5 py-1.5 text-sm font-medium text-danger transition-colors hover:bg-danger/15 disabled:opacity-50 dark:border-danger/25 dark:bg-danger/15 dark:text-red-300 dark:hover:bg-danger/20"
            >
              {isDeleting ? 'Removing...' : 'Remove Override'}
            </button>

            <div className="flex gap-2">
              <button
                type="button"
                onClick={onClose}
                disabled={busy}
                className="rounded-lg px-3 py-2 text-sm font-medium text-muted hover:text-foreground disabled:opacity-50"
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={handleSave}
                disabled={busy}
                className="rounded-lg bg-accent px-4 py-2 text-sm font-medium text-white transition-colors hover:bg-accent/90 disabled:opacity-50 dark:text-background"
              >
                {isSaving ? 'Saving...' : 'Save Override'}
              </button>
            </div>
          </div>
        </DialogPanel>
      </div>
    </Dialog>
  );
}
