import { type JSX, useState, useEffect } from 'react';
import { Dialog, DialogPanel, DialogTitle, DialogBackdrop } from '@headlessui/react';
import { TrashIcon } from '@heroicons/react/24/outline';
import type { IntervalTypeTransformation } from '@/api/types.gen';
import { TransformationSelector } from '@/components/Forms/TransformationSelector';
import { reverseTransformValue } from '@/utils/interval-transform';

interface DeletePeriodDialogProps {
  open: boolean;
  onClose: () => void;
  onDelete: (startPos: number, endPos: number, cascade: boolean) => void;
  isDeleting?: boolean;
  transformations?: IntervalTypeTransformation[];
}

const inputClassName =
  'mt-1 block w-full rounded-lg border border-border/60 bg-background px-3 py-2 text-sm text-foreground placeholder:text-muted/50 focus:border-accent focus:ring-1 focus:ring-accent focus:outline-hidden disabled:opacity-50';

export function DeletePeriodDialog({
  open,
  onClose,
  onDelete,
  isDeleting,
  transformations,
}: DeletePeriodDialogProps): JSX.Element {
  const [startPos, setStartPos] = useState('');
  const [endPos, setEndPos] = useState('');
  const [cascade, setCascade] = useState(false);
  const [selectedTransformationIndex, setSelectedTransformationIndex] = useState(0);
  const [startDatetime, setStartDatetime] = useState('');
  const [endDatetime, setEndDatetime] = useState('');

  const hasTransformations = transformations && transformations.length > 0;
  const currentTransformation = hasTransformations ? transformations[selectedTransformationIndex] : undefined;
  const isDatetime = currentTransformation?.format === 'datetime';

  useEffect(() => {
    if (open) {
      setStartPos('');
      setEndPos('');
      setCascade(false);
      setSelectedTransformationIndex(0);
      setStartDatetime('');
      setEndDatetime('');
    }
  }, [open]);

  const handleTransformationChange = (index: number): void => {
    setSelectedTransformationIndex(index);
    setStartPos('');
    setEndPos('');
    setStartDatetime('');
    setEndDatetime('');
  };

  const handleSubmit = (e: React.FormEvent): void => {
    e.preventDefault();
    if (isDeleting) return;

    if (isDatetime && currentTransformation) {
      if (!startDatetime || !endDatetime) return;

      const startMs = new Date(startDatetime).getTime();
      const endMs = new Date(endDatetime).getTime();

      if (startMs >= endMs) return;

      const start = reverseTransformValue(startMs, currentTransformation);
      const end = reverseTransformValue(endMs, currentTransformation);

      if (start >= end) return;

      onDelete(start, end, cascade);
    } else {
      if (startPos.trim() === '' || endPos.trim() === '') return;

      const start = Number(startPos);
      const end = Number(endPos);

      if (start >= end) return;

      onDelete(start, end, cascade);
    }
  };

  const valid = isDatetime
    ? startDatetime !== '' && endDatetime !== '' && new Date(startDatetime).getTime() < new Date(endDatetime).getTime()
    : startPos.trim() !== '' && endPos.trim() !== '' && Number(startPos) < Number(endPos);

  return (
    <Dialog open={open} onClose={onClose} className="relative z-50">
      <DialogBackdrop className="fixed inset-0 bg-black/40 backdrop-blur-xs" />
      <div className="fixed inset-0 flex items-center justify-center p-4">
        <DialogPanel className="w-full max-w-sm rounded-xl border border-border/50 bg-surface p-6 shadow-lg">
          <div className="flex items-center gap-3">
            <div className="flex size-10 items-center justify-center rounded-lg bg-danger/10">
              <TrashIcon className="size-5 text-danger" />
            </div>
            <DialogTitle className="text-lg font-semibold text-foreground">Delete Period</DialogTitle>
          </div>

          <form onSubmit={handleSubmit} className="mt-4 space-y-4">
            {hasTransformations && (
              <TransformationSelector
                transformations={transformations}
                selectedIndex={selectedTransformationIndex}
                onSelect={handleTransformationChange}
                compact
              />
            )}

            {isDatetime ? (
              <>
                <div>
                  <label htmlFor="delete-start-dt" className="block text-sm font-medium text-muted">
                    Start Date/Time
                  </label>
                  <input
                    id="delete-start-dt"
                    type="datetime-local"
                    value={startDatetime}
                    onChange={e => setStartDatetime(e.target.value)}
                    disabled={isDeleting}
                    className={inputClassName}
                  />
                </div>

                <div>
                  <label htmlFor="delete-end-dt" className="block text-sm font-medium text-muted">
                    End Date/Time
                  </label>
                  <input
                    id="delete-end-dt"
                    type="datetime-local"
                    value={endDatetime}
                    onChange={e => setEndDatetime(e.target.value)}
                    disabled={isDeleting}
                    className={inputClassName}
                  />
                </div>
              </>
            ) : (
              <>
                <div>
                  <label htmlFor="delete-start" className="block text-sm font-medium text-muted">
                    Start Position
                  </label>
                  <input
                    id="delete-start"
                    type="number"
                    value={startPos}
                    onChange={e => setStartPos(e.target.value)}
                    disabled={isDeleting}
                    className={inputClassName}
                    placeholder="0"
                    min={0}
                  />
                </div>

                <div>
                  <label htmlFor="delete-end" className="block text-sm font-medium text-muted">
                    End Position
                  </label>
                  <input
                    id="delete-end"
                    type="number"
                    value={endPos}
                    onChange={e => setEndPos(e.target.value)}
                    disabled={isDeleting}
                    className={inputClassName}
                    placeholder="0"
                    min={0}
                  />
                </div>
              </>
            )}

            <label className="flex items-start gap-2.5">
              <input
                type="checkbox"
                checked={cascade}
                onChange={e => setCascade(e.target.checked)}
                disabled={isDeleting}
                className="mt-0.5 size-4 rounded-xs border-border/60 text-accent focus:ring-accent disabled:opacity-50"
              />
              <span className="text-sm text-muted">
                <span className="font-medium text-foreground">Cascade to all dependents</span>
                <br />
                Also delete this period from all downstream incremental models in the DAG.
              </span>
            </label>

            <div className="flex items-center justify-end gap-2">
              <button
                type="button"
                onClick={onClose}
                disabled={isDeleting}
                className="rounded-lg px-3 py-2 text-sm font-medium text-muted hover:text-foreground disabled:opacity-50"
              >
                Cancel
              </button>
              <button
                type="submit"
                disabled={!valid || isDeleting}
                className="rounded-lg bg-danger px-4 py-2 text-sm font-medium text-white transition-colors hover:bg-danger/90 disabled:opacity-50"
              >
                {isDeleting ? 'Deleting...' : 'Delete'}
              </button>
            </div>
          </form>
        </DialogPanel>
      </div>
    </Dialog>
  );
}
