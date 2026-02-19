import { type JSX, useState } from 'react';
import {
  ArrowPathIcon,
  PencilSquareIcon,
  TrashIcon,
  AdjustmentsHorizontalIcon,
  PlayIcon,
} from '@heroicons/react/24/outline';
import { WrenchScrewdriverIcon } from '@heroicons/react/24/solid';
import type { IntervalTypeTransformation, Range } from '@/api/types.gen';
import { useAuth } from '@/hooks/useAuth';
import { useNotification } from '@/hooks/useNotification';
import { adminFetch } from '@/utils/admin-api';
import { EditBoundsDialog } from '@/components/Overlays/EditBoundsDialog';
import { DeletePeriodDialog } from '@/components/Overlays/DeletePeriodDialog';
import { ConfigOverrideDialog } from '@/components/Overlays/ConfigOverrideDialog';

interface ConfigOverride {
  model_id: string;
  model_type: string;
  enabled?: boolean;
  override: Record<string, unknown> | null;
  updated_at: string;
}

interface ModelAdminActionsProps {
  modelId: string;
  modelType: 'incremental' | 'scheduled' | 'external';
  currentMin?: number;
  currentMax?: number;
  onBoundsChanged?: () => void;
  transformations?: IntervalTypeTransformation[];
  coverageRanges?: Array<Range>;
  currentOverride?: ConfigOverride | null;
  baseConfig?: Record<string, unknown> | null;
  onOverrideChanged?: () => void;
}

export function ModelAdminActions({
  modelId,
  modelType,
  currentMin,
  currentMax,
  onBoundsChanged,
  transformations,
  coverageRanges,
  currentOverride,
  baseConfig,
  onOverrideChanged,
}: ModelAdminActionsProps): JSX.Element | null {
  const { managementEnabled, session } = useAuth();
  const { showSuccess, showError, showNotification } = useNotification();
  const [loading, setLoading] = useState(false);
  const [boundsOpen, setBoundsOpen] = useState(false);
  const [isSaving, setIsSaving] = useState(false);
  const [isDeleting, setIsDeleting] = useState(false);
  const [refreshLoading, setRefreshLoading] = useState(false);
  const [deletePeriodOpen, setDeletePeriodOpen] = useState(false);
  const [isDeletingPeriod, setIsDeletingPeriod] = useState(false);
  const [configOverrideOpen, setConfigOverrideOpen] = useState(false);
  const [isSavingOverride, setIsSavingOverride] = useState(false);
  const [isDeletingOverride, setIsDeletingOverride] = useState(false);
  const [runNowLoading, setRunNowLoading] = useState(false);

  if (!managementEnabled || !session?.authenticated) {
    return null;
  }

  const encodedId = encodeURIComponent(modelId);

  const isModelDisabled = currentOverride?.enabled === false;
  const isTransformation = modelType === 'incremental' || modelType === 'scheduled';

  const handleConsolidate = async (): Promise<void> => {
    setLoading(true);

    try {
      const resp = await adminFetch(`/api/v1/admin/models/${encodedId}/consolidate`, {
        method: 'POST',
      });

      if (resp.ok) {
        const data = await resp.json();
        showSuccess(`Consolidated ${data.ranges_merged ?? 0} ranges`);
      } else {
        const errorData = await resp.json().catch(() => ({ error: 'Request failed' }));
        showError(errorData.error ?? `Error ${resp.status}`);
      }
    } catch {
      showError('Network error');
    } finally {
      setLoading(false);
    }
  };

  const handleSaveBounds = async (min: number, max: number): Promise<void> => {
    setIsSaving(true);

    try {
      const resp = await adminFetch(`/api/v1/admin/models/${encodedId}/bounds`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ min, max }),
      });

      if (resp.ok) {
        showSuccess('Bounds updated');
        setBoundsOpen(false);
        onBoundsChanged?.();
      } else {
        const errorData = await resp.json().catch(() => ({ error: 'Request failed' }));
        showError(errorData.error ?? `Error ${resp.status}`);
      }
    } catch {
      showError('Network error');
    } finally {
      setIsSaving(false);
    }
  };

  const handleDeleteBounds = async (): Promise<void> => {
    setIsDeleting(true);

    try {
      const resp = await adminFetch(`/api/v1/admin/models/${encodedId}/bounds`, {
        method: 'DELETE',
      });

      if (resp.ok) {
        showSuccess('Bounds deleted');
        setBoundsOpen(false);
        onBoundsChanged?.();
      } else {
        const errorData = await resp.json().catch(() => ({ error: 'Request failed' }));
        showError(errorData.error ?? `Error ${resp.status}`);
      }
    } catch {
      showError('Network error');
    } finally {
      setIsDeleting(false);
    }
  };

  const handleRefreshBounds = async (): Promise<void> => {
    setRefreshLoading(true);

    try {
      const resp = await adminFetch(`/api/v1/admin/models/${encodedId}/refresh-bounds`, {
        method: 'POST',
      });

      if (resp.ok) {
        showSuccess('Bounds refresh enqueued');
      } else {
        const errorData = await resp.json().catch(() => ({ error: 'Request failed' }));
        showError(errorData.error ?? `Error ${resp.status}`);
      }
    } catch {
      showError('Network error');
    } finally {
      setRefreshLoading(false);
    }
  };

  const handleDeletePeriod = async (startPos: number, endPos: number, cascade: boolean): Promise<void> => {
    setIsDeletingPeriod(true);

    try {
      const resp = await adminFetch(`/api/v1/admin/models/${encodedId}/delete-period`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ start_pos: startPos, end_pos: endPos, cascade }),
      });

      if (resp.ok) {
        const data = await resp.json();
        const cascadeCount = data.cascade_results?.length ?? 0;
        const msg =
          cascadeCount > 0
            ? `Deleted ${data.deleted_rows} rows (+ ${cascadeCount} cascaded models)`
            : `Deleted ${data.deleted_rows} rows`;
        showSuccess(msg);
        setDeletePeriodOpen(false);
      } else {
        const errorData = await resp.json().catch(() => ({ error: 'Request failed' }));
        showError(errorData.error ?? `Error ${resp.status}`);
      }
    } catch {
      showError('Network error');
    } finally {
      setIsDeletingPeriod(false);
    }
  };

  const handleRunNow = async (): Promise<void> => {
    if (modelType !== 'scheduled' || isModelDisabled) return;

    setRunNowLoading(true);

    try {
      const resp = await adminFetch(`/api/v1/admin/models/${encodedId}/run-now`, {
        method: 'POST',
      });

      if (resp.ok) {
        showSuccess('Scheduled run enqueued');
      } else if (resp.status === 409) {
        const errorData = await resp.json().catch(() => ({ error: 'Scheduled run already in progress' }));
        showNotification({
          message: errorData.error ?? 'Scheduled run already in progress',
          variant: 'warning',
        });
      } else {
        const errorData = await resp.json().catch(() => ({ error: 'Request failed' }));
        showError(errorData.error ?? `Error ${resp.status}`);
      }
    } catch {
      showError('Network error');
    } finally {
      setRunNowLoading(false);
    }
  };

  const handleSaveOverride = async (json: string): Promise<void> => {
    setIsSavingOverride(true);

    try {
      const parsed = JSON.parse(json);
      const body: Record<string, unknown> = {};

      if (parsed.enabled != null && isTransformation) {
        body.enabled = parsed.enabled;
      }

      if (parsed.config != null) {
        body.config = parsed.config;
      }

      const resp = await adminFetch(`/api/v1/admin/models/${encodedId}/config-override`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });

      if (resp.ok) {
        showSuccess('Config override saved');
        setConfigOverrideOpen(false);
        onOverrideChanged?.();
      } else {
        const errorData = await resp.json().catch(() => ({ error: 'Request failed' }));
        showError(errorData.error ?? `Error ${resp.status}`);
      }
    } catch {
      showError('Invalid JSON or network error');
    } finally {
      setIsSavingOverride(false);
    }
  };

  const handleDeleteOverride = async (): Promise<void> => {
    setIsDeletingOverride(true);

    try {
      const resp = await adminFetch(`/api/v1/admin/models/${encodedId}/config-override`, {
        method: 'DELETE',
      });

      if (resp.ok) {
        showSuccess('Config override removed');
        setConfigOverrideOpen(false);
        onOverrideChanged?.();
      } else {
        const errorData = await resp.json().catch(() => ({ error: 'Request failed' }));
        showError(errorData.error ?? `Error ${resp.status}`);
      }
    } catch {
      showError('Network error');
    } finally {
      setIsDeletingOverride(false);
    }
  };

  const handleToggleEnabled = async (): Promise<void> => {
    if (!isTransformation) return;

    const newEnabled = isModelDisabled;

    // If re-enabling and there's no other override data, just delete the override entirely
    if (newEnabled && (!currentOverride?.override || Object.keys(currentOverride.override).length === 0)) {
      await handleDeleteOverride();
      return;
    }

    setIsSavingOverride(true);

    try {
      const body: Record<string, unknown> = { enabled: newEnabled };

      if (currentOverride?.override) {
        body.config = currentOverride.override;
      }

      const resp = await adminFetch(`/api/v1/admin/models/${encodedId}/config-override`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });

      if (resp.ok) {
        showSuccess(newEnabled ? 'Model enabled' : 'Model disabled');
        onOverrideChanged?.();
      } else {
        const errorData = await resp.json().catch(() => ({ error: 'Request failed' }));
        showError(errorData.error ?? `Error ${resp.status}`);
      }
    } catch {
      showError('Network error');
    } finally {
      setIsSavingOverride(false);
    }
  };

  const overrideBusy = isSavingOverride || isDeletingOverride;

  const adminBar = (
    <div className="mt-4 mb-4 flex flex-wrap items-center gap-3 rounded-lg border border-border/40 bg-surface/50 px-4 py-2">
      <WrenchScrewdriverIcon className="size-4 text-accent" />
      <span className="text-xs font-semibold tracking-wide text-accent uppercase">Admin</span>
      <div className="mx-1 h-4 w-px bg-border/50" />

      {/* Enable/disable toggle for transformations */}
      {isTransformation && (
        <button
          type="button"
          onClick={() => void handleToggleEnabled()}
          disabled={overrideBusy}
          className={`inline-flex items-center gap-1.5 rounded-md border px-2.5 py-1 text-xs font-medium shadow-xs transition-all disabled:opacity-50 ${
            isModelDisabled
              ? 'border-danger/50 bg-danger/10 text-danger hover:bg-danger/20'
              : 'border-success/50 bg-success/10 text-success hover:bg-success/20'
          }`}
        >
          <span className={`size-2 rounded-full ${isModelDisabled ? 'bg-danger' : 'bg-success'}`} />
          {isModelDisabled ? 'Disabled' : 'Enabled'}
        </button>
      )}

      {modelType === 'scheduled' && (
        <button
          type="button"
          onClick={() => void handleRunNow()}
          disabled={runNowLoading || isModelDisabled}
          className="inline-flex items-center gap-1.5 rounded-md border border-border/50 bg-surface px-2.5 py-1 text-xs font-medium text-foreground shadow-xs transition-all hover:bg-secondary/50 hover:text-accent disabled:opacity-50"
        >
          <PlayIcon className="size-3.5" />
          {runNowLoading ? 'Running...' : 'Run Now'}
        </button>
      )}

      {/* Config override button */}
      <button
        type="button"
        onClick={() => setConfigOverrideOpen(true)}
        className={`inline-flex items-center gap-1.5 rounded-md border border-border/50 bg-surface px-2.5 py-1 text-xs font-medium shadow-xs transition-all hover:bg-secondary/50 hover:text-accent disabled:opacity-50 ${
          currentOverride ? 'text-warning' : 'text-foreground'
        }`}
      >
        <AdjustmentsHorizontalIcon className="size-3.5" />
        {currentOverride ? 'Override Active' : 'Config Override'}
      </button>
    </div>
  );

  const overrideDialog = (
    <ConfigOverrideDialog
      open={configOverrideOpen}
      onClose={() => setConfigOverrideOpen(false)}
      onSave={json => void handleSaveOverride(json)}
      onDelete={() => void handleDeleteOverride()}
      modelId={modelId}
      modelType={modelType}
      currentOverride={currentOverride}
      baseConfig={baseConfig}
      isSaving={isSavingOverride}
      isDeleting={isDeletingOverride}
    />
  );

  if (modelType === 'external') {
    return (
      <>
        <div className="mt-4 mb-4 flex flex-wrap items-center gap-3 rounded-lg border border-border/40 bg-surface/50 px-4 py-2">
          <WrenchScrewdriverIcon className="size-4 text-accent" />
          <span className="text-xs font-semibold tracking-wide text-accent uppercase">Admin</span>
          <div className="mx-1 h-4 w-px bg-border/50" />
          <button
            type="button"
            onClick={() => setBoundsOpen(true)}
            className="inline-flex items-center gap-1.5 rounded-md border border-border/50 bg-surface px-2.5 py-1 text-xs font-medium text-foreground shadow-xs transition-all hover:bg-secondary/50 hover:text-accent disabled:opacity-50"
          >
            <PencilSquareIcon className="size-3.5" />
            Edit Bounds
          </button>
          <button
            type="button"
            onClick={() => void handleRefreshBounds()}
            disabled={refreshLoading}
            className="inline-flex items-center gap-1.5 rounded-md border border-border/50 bg-surface px-2.5 py-1 text-xs font-medium text-foreground shadow-xs transition-all hover:bg-secondary/50 hover:text-accent disabled:opacity-50"
          >
            <ArrowPathIcon className={`size-3.5 ${refreshLoading ? 'animate-spin' : ''}`} />
            Refresh Bounds
          </button>
          {/* Config override button */}
          <button
            type="button"
            onClick={() => setConfigOverrideOpen(true)}
            className={`inline-flex items-center gap-1.5 rounded-md border border-border/50 bg-surface px-2.5 py-1 text-xs font-medium shadow-xs transition-all hover:bg-secondary/50 hover:text-accent disabled:opacity-50 ${
              currentOverride ? 'text-warning' : 'text-foreground'
            }`}
          >
            <AdjustmentsHorizontalIcon className="size-3.5" />
            {currentOverride ? 'Override Active' : 'Config Override'}
          </button>
        </div>
        <EditBoundsDialog
          open={boundsOpen}
          onClose={() => setBoundsOpen(false)}
          onSave={(min, max) => void handleSaveBounds(min, max)}
          onDelete={() => void handleDeleteBounds()}
          currentMin={currentMin}
          currentMax={currentMax}
          isSaving={isSaving}
          isDeleting={isDeleting}
        />
        {overrideDialog}
      </>
    );
  }

  if (modelType === 'scheduled') {
    return (
      <>
        {adminBar}
        {overrideDialog}
      </>
    );
  }

  // Incremental model
  return (
    <>
      <div className="mt-4 mb-4 flex flex-wrap items-center gap-3 rounded-lg border border-border/40 bg-surface/50 px-4 py-2">
        <WrenchScrewdriverIcon className="size-4 text-accent" />
        <span className="text-xs font-semibold tracking-wide text-accent uppercase">Admin</span>
        <div className="mx-1 h-4 w-px bg-border/50" />

        {/* Enable/disable toggle */}
        <button
          type="button"
          onClick={() => void handleToggleEnabled()}
          disabled={overrideBusy}
          className={`inline-flex items-center gap-1.5 rounded-md border px-2.5 py-1 text-xs font-medium shadow-xs transition-all disabled:opacity-50 ${
            isModelDisabled
              ? 'border-danger/50 bg-danger/10 text-danger hover:bg-danger/20'
              : 'border-success/50 bg-success/10 text-success hover:bg-success/20'
          }`}
        >
          <span className={`size-2 rounded-full ${isModelDisabled ? 'bg-danger' : 'bg-success'}`} />
          {isModelDisabled ? 'Disabled' : 'Enabled'}
        </button>

        <button
          type="button"
          onClick={() => void handleConsolidate()}
          disabled={loading}
          className="inline-flex items-center gap-1.5 rounded-md border border-border/50 bg-surface px-2.5 py-1 text-xs font-medium text-foreground shadow-xs transition-all hover:bg-secondary/50 hover:text-accent disabled:opacity-50"
        >
          <ArrowPathIcon className={`size-3.5 ${loading ? 'animate-spin' : ''}`} />
          Consolidate
        </button>
        <button
          type="button"
          onClick={() => setDeletePeriodOpen(true)}
          className="inline-flex items-center gap-1.5 rounded-md border border-border/50 bg-surface px-2.5 py-1 text-xs font-medium text-foreground shadow-xs transition-all hover:bg-secondary/50 hover:text-danger disabled:opacity-50"
        >
          <TrashIcon className="size-3.5" />
          Delete Period
        </button>

        {/* Config override button */}
        <button
          type="button"
          onClick={() => setConfigOverrideOpen(true)}
          className={`inline-flex items-center gap-1.5 rounded-md border border-border/50 bg-surface px-2.5 py-1 text-xs font-medium shadow-xs transition-all hover:bg-secondary/50 hover:text-accent disabled:opacity-50 ${
            currentOverride ? 'text-warning' : 'text-foreground'
          }`}
        >
          <AdjustmentsHorizontalIcon className="size-3.5" />
          {currentOverride ? 'Override Active' : 'Config Override'}
        </button>
      </div>
      <DeletePeriodDialog
        open={deletePeriodOpen}
        onClose={() => setDeletePeriodOpen(false)}
        onDelete={(startPos, endPos, cascade) => void handleDeletePeriod(startPos, endPos, cascade)}
        isDeleting={isDeletingPeriod}
        transformations={transformations}
        coverageRanges={coverageRanges}
      />
      {overrideDialog}
    </>
  );
}
