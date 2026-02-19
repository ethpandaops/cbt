import { type JSX, useState } from 'react';
import { ArrowPathIcon, PencilSquareIcon, TrashIcon } from '@heroicons/react/24/outline';
import { WrenchScrewdriverIcon } from '@heroicons/react/24/solid';
import type { IntervalTypeTransformation } from '@/api/types.gen';
import { useAuth } from '@/hooks/useAuth';
import { useNotification } from '@/hooks/useNotification';
import { adminFetch } from '@/utils/admin-api';
import { EditBoundsDialog } from '@/components/Overlays/EditBoundsDialog';
import { DeletePeriodDialog } from '@/components/Overlays/DeletePeriodDialog';

interface ModelAdminActionsProps {
  modelId: string;
  modelType: 'incremental' | 'scheduled' | 'external';
  currentMin?: number;
  currentMax?: number;
  onBoundsChanged?: () => void;
  transformations?: IntervalTypeTransformation[];
}

export function ModelAdminActions({
  modelId,
  modelType,
  currentMin,
  currentMax,
  onBoundsChanged,
  transformations,
}: ModelAdminActionsProps): JSX.Element | null {
  const { managementEnabled, session } = useAuth();
  const { showSuccess, showError } = useNotification();
  const [loading, setLoading] = useState(false);
  const [boundsOpen, setBoundsOpen] = useState(false);
  const [isSaving, setIsSaving] = useState(false);
  const [isDeleting, setIsDeleting] = useState(false);
  const [refreshLoading, setRefreshLoading] = useState(false);
  const [deletePeriodOpen, setDeletePeriodOpen] = useState(false);
  const [isDeletingPeriod, setIsDeletingPeriod] = useState(false);

  if (!managementEnabled || !session?.authenticated) {
    return null;
  }

  if (modelType === 'scheduled') {
    return null;
  }

  const encodedId = encodeURIComponent(modelId);

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

  if (modelType === 'external') {
    return (
      <>
        <div className="mt-4 mb-4 flex items-center gap-3 rounded-lg border border-border/40 bg-surface/50 px-4 py-2">
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
      </>
    );
  }

  // Incremental model
  return (
    <>
      <div className="mt-4 mb-4 flex items-center gap-3 rounded-lg border border-border/40 bg-surface/50 px-4 py-2">
        <WrenchScrewdriverIcon className="size-4 text-accent" />
        <span className="text-xs font-semibold tracking-wide text-accent uppercase">Admin</span>
        <div className="mx-1 h-4 w-px bg-border/50" />
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
      </div>
      <DeletePeriodDialog
        open={deletePeriodOpen}
        onClose={() => setDeletePeriodOpen(false)}
        onDelete={(startPos, endPos, cascade) => void handleDeletePeriod(startPos, endPos, cascade)}
        isDeleting={isDeletingPeriod}
        transformations={transformations}
      />
    </>
  );
}
