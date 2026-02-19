import { type JSX, useState, useEffect } from 'react';
import { Dialog, DialogPanel, DialogTitle, DialogBackdrop } from '@headlessui/react';
import { PencilSquareIcon } from '@heroicons/react/24/outline';

interface EditBoundsDialogProps {
  open: boolean;
  onClose: () => void;
  onSave: (min: number, max: number) => void;
  onDelete: () => void;
  currentMin?: number;
  currentMax?: number;
  isSaving?: boolean;
  isDeleting?: boolean;
}

export function EditBoundsDialog({
  open,
  onClose,
  onSave,
  onDelete,
  currentMin,
  currentMax,
  isSaving,
  isDeleting,
}: EditBoundsDialogProps): JSX.Element {
  const [min, setMin] = useState('');
  const [max, setMax] = useState('');

  const busy = isSaving || isDeleting;

  useEffect(() => {
    if (open) {
      setMin(currentMin != null ? String(currentMin) : '');
      setMax(currentMax != null ? String(currentMax) : '');
    }
  }, [open, currentMin, currentMax]);

  const handleSave = (e: React.FormEvent): void => {
    e.preventDefault();

    if (min.trim() === '' || max.trim() === '' || busy) return;

    onSave(Number(min), Number(max));
  };

  return (
    <Dialog open={open} onClose={onClose} className="relative z-50">
      <DialogBackdrop className="fixed inset-0 bg-primary/28 backdrop-blur-sm" />
      <div className="fixed inset-0 flex items-center justify-center p-4">
        <DialogPanel className="glass-surface w-full max-w-sm p-6">
          <div className="flex items-center gap-3">
            <div className="flex size-10 items-center justify-center rounded-lg border border-accent/30 bg-accent/12 ring-1 ring-accent/20">
              <PencilSquareIcon className="size-5 text-accent" />
            </div>
            <DialogTitle className="text-lg font-semibold text-foreground">Edit Bounds</DialogTitle>
          </div>

          <form onSubmit={handleSave} className="mt-4 space-y-4">
            <div>
              <label htmlFor="bounds-min" className="block text-sm font-medium text-muted">
                Min Position
              </label>
              <input
                id="bounds-min"
                type="number"
                value={min}
                onChange={e => setMin(e.target.value)}
                disabled={busy}
                className="mt-1 block w-full rounded-lg border border-border/65 bg-surface/90 px-3 py-2 text-sm text-foreground shadow-xs ring-1 ring-border/35 placeholder:text-muted/60 focus:border-accent focus:ring-2 focus:ring-accent/45 focus:outline-hidden disabled:opacity-50"
                placeholder="0"
                min={0}
              />
            </div>

            <div>
              <label htmlFor="bounds-max" className="block text-sm font-medium text-muted">
                Max Position
              </label>
              <input
                id="bounds-max"
                type="number"
                value={max}
                onChange={e => setMax(e.target.value)}
                disabled={busy}
                className="mt-1 block w-full rounded-lg border border-border/65 bg-surface/90 px-3 py-2 text-sm text-foreground shadow-xs ring-1 ring-border/35 placeholder:text-muted/60 focus:border-accent focus:ring-2 focus:ring-accent/45 focus:outline-hidden disabled:opacity-50"
                placeholder="0"
                min={0}
              />
            </div>

            <div className="flex items-center justify-between">
              <button
                type="button"
                onClick={onDelete}
                disabled={busy}
                className="text-sm font-medium text-danger hover:text-danger/80 disabled:opacity-50"
              >
                {isDeleting ? 'Deleting...' : 'Delete Bounds'}
              </button>

              <div className="flex gap-2">
                <button
                  type="button"
                  onClick={onClose}
                  disabled={busy}
                  className="glass-control px-3 py-2 text-sm font-medium text-muted hover:text-primary disabled:opacity-50"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  disabled={min.trim() === '' || max.trim() === '' || busy}
                  className="rounded-lg border border-accent/50 bg-accent px-4 py-2 text-sm font-medium text-white shadow-xs transition-colors hover:bg-accent/90 disabled:opacity-50 dark:text-background"
                >
                  {isSaving ? 'Saving...' : 'Save'}
                </button>
              </div>
            </div>
          </form>
        </DialogPanel>
      </div>
    </Dialog>
  );
}
