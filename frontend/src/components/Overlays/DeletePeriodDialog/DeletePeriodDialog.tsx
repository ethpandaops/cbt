import { type JSX, useState, useEffect, useMemo, useRef } from 'react';
import { Dialog, DialogPanel, DialogTitle, DialogBackdrop } from '@headlessui/react';
import { TrashIcon } from '@heroicons/react/24/outline';
import type { IntervalTypeTransformation, Range } from '@/api/types.gen';
import { TransformationSelector } from '@/components/Forms/TransformationSelector';
import { CoverageBar } from '@/components/Domain/Coverage/CoverageBar';
import { transformValue, formatValue } from '@/utils/interval-transform';
import { findCoverageAtPosition, findGapAtPosition, mergeRanges } from '@/utils/coverage-helpers';
import { DeleteRangeSlider } from './DeleteRangeSlider';

interface DeletePeriodDialogProps {
  open: boolean;
  onClose: () => void;
  onDelete: (startPos: number, endPos: number, cascade: boolean) => void;
  isDeleting?: boolean;
  transformations?: IntervalTypeTransformation[];
  coverageRanges?: Array<Range>;
}

const inputClassName =
  'mt-1 block w-full rounded-lg border border-border/60 bg-background px-3 py-2 text-sm text-foreground placeholder:text-muted/50 focus:border-accent focus:ring-1 focus:ring-accent focus:outline-hidden disabled:opacity-50';

export function DeletePeriodDialog({
  open,
  onClose,
  onDelete,
  isDeleting,
  transformations,
  coverageRanges,
}: DeletePeriodDialogProps): JSX.Element {
  const [startPos, setStartPos] = useState('');
  const [endPos, setEndPos] = useState('');
  const [cascade, setCascade] = useState(false);
  const [selectedTransformationIndex, setSelectedTransformationIndex] = useState(0);
  const [sliderStart, setSliderStart] = useState(0);
  const [sliderEnd, setSliderEnd] = useState(0);

  // Tooltip state
  const [tooltipText, setTooltipText] = useState<string | null>(null);
  const [tooltipX, setTooltipX] = useState(0);
  const [tooltipMissing, setTooltipMissing] = useState(false);
  const barRef = useRef<HTMLDivElement>(null);

  const hasTransformations = transformations && transformations.length > 0;
  const currentTransformation = hasTransformations ? transformations[selectedTransformationIndex] : undefined;

  // Compute global min/max from coverage ranges
  const { globalMin, globalMax } = useMemo(() => {
    if (!coverageRanges || coverageRanges.length === 0) {
      return { globalMin: 0, globalMax: 0 };
    }

    let min = Infinity;
    let max = -Infinity;

    for (const r of coverageRanges) {
      if (r.position < min) min = r.position;
      const end = r.position + r.interval;
      if (end > max) max = end;
    }

    return { globalMin: min, globalMax: max };
  }, [coverageRanges]);

  const hasCoverage = coverageRanges && coverageRanges.length > 0 && globalMin !== globalMax;

  const mergedRanges = useMemo(() => {
    if (!coverageRanges || coverageRanges.length === 0) return [];
    return mergeRanges(coverageRanges);
  }, [coverageRanges]);

  // Format a raw position using the current transformation
  const formatPosition = (pos: number): string => {
    if (currentTransformation) {
      const transformed = transformValue(pos, currentTransformation);
      return formatValue(transformed, currentTransformation.format);
    }
    return pos.toLocaleString();
  };

  // Format min/max labels
  const minLabel = useMemo(() => {
    if (!hasCoverage) return '';
    return formatPosition(globalMin);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [hasCoverage, globalMin, currentTransformation]);

  const maxLabel = useMemo(() => {
    if (!hasCoverage) return '';
    return formatPosition(globalMax);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [hasCoverage, globalMax, currentTransformation]);

  // Conversion display for the current input values
  const startConverted = useMemo(() => {
    const raw = startPos.trim() !== '' ? Number(startPos) : hasCoverage ? globalMin : null;
    if (raw === null || isNaN(raw)) return '—';
    return formatPosition(raw);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [currentTransformation, startPos, hasCoverage, globalMin]);

  const endConverted = useMemo(() => {
    const raw = endPos.trim() !== '' ? Number(endPos) : hasCoverage ? globalMax : null;
    if (raw === null || isNaN(raw)) return '—';
    return formatPosition(raw);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [currentTransformation, endPos, hasCoverage, globalMax]);

  // Reset on open
  useEffect(() => {
    if (open) {
      setStartPos('');
      setEndPos('');
      setCascade(false);
      setSelectedTransformationIndex(0);
      setSliderStart(globalMin);
      setSliderEnd(globalMax);
      setTooltipText(null);
    }
  }, [open, globalMin, globalMax]);

  // Coverage bar hover handlers
  const handleCoverageHover = (position: number, mouseX: number): void => {
    if (!barRef.current) return;

    const barRect = barRef.current.getBoundingClientRect();
    setTooltipX(mouseX - barRect.left);

    const coverage = findCoverageAtPosition(mergedRanges, position);
    if (coverage) {
      const rangeMin = coverage.position;
      const rangeMax = coverage.position + coverage.interval;
      setTooltipText(`${formatPosition(rangeMin)} → ${formatPosition(rangeMax)}`);
      setTooltipMissing(false);
    } else {
      const gap = findGapAtPosition(coverageRanges, position, globalMin, globalMax);
      if (gap) {
        const gapMin = gap.position;
        const gapMax = gap.position + gap.interval;
        setTooltipText(`Missing: ${formatPosition(gapMin)} → ${formatPosition(gapMax)}`);
      } else {
        setTooltipText('Missing');
      }
      setTooltipMissing(true);
    }
  };

  const handleCoverageLeave = (): void => {
    setTooltipText(null);
  };

  // Slider -> Inputs sync
  const handleSliderChange = (start: number, end: number): void => {
    setSliderStart(start);
    setSliderEnd(end);
    setStartPos(String(start));
    setEndPos(String(end));
  };

  // Input -> Slider sync
  const handleStartPosChange = (value: string): void => {
    setStartPos(value);
    if (hasCoverage && value.trim() !== '') {
      const num = Number(value);
      if (!isNaN(num)) {
        setSliderStart(Math.max(globalMin, Math.min(globalMax, num)));
      }
    }
  };

  const handleEndPosChange = (value: string): void => {
    setEndPos(value);
    if (hasCoverage && value.trim() !== '') {
      const num = Number(value);
      if (!isNaN(num)) {
        setSliderEnd(Math.max(globalMin, Math.min(globalMax, num)));
      }
    }
  };

  const handleSubmit = (e: React.FormEvent): void => {
    e.preventDefault();
    if (isDeleting) return;
    if (startPos.trim() === '' || endPos.trim() === '') return;

    const start = Number(startPos);
    const end = Number(endPos);

    if (start >= end) return;

    onDelete(start, end, cascade);
  };

  const valid = startPos.trim() !== '' && endPos.trim() !== '' && Number(startPos) < Number(endPos);

  return (
    <Dialog open={open} onClose={onClose} className="relative z-50">
      <DialogBackdrop className="fixed inset-0 bg-black/40 backdrop-blur-xs" />
      <div className="fixed inset-0 flex items-center justify-center p-4">
        <DialogPanel className="w-full max-w-screen-2xl rounded-xl border border-border/50 bg-surface p-6 shadow-lg">
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
                onSelect={setSelectedTransformationIndex}
                compact
              />
            )}

            {hasCoverage && (
              <div className="space-y-2">
                <label className="block text-sm font-medium text-muted">Coverage Range</label>
                <div ref={barRef} className="relative">
                  <CoverageBar
                    ranges={coverageRanges}
                    zoomStart={globalMin}
                    zoomEnd={globalMax}
                    type="transformation"
                    height={24}
                    onCoverageHover={handleCoverageHover}
                    onCoverageLeave={handleCoverageLeave}
                  />
                  {tooltipText && (
                    <div
                      className={`pointer-events-none absolute -top-8 z-10 rounded px-1.5 py-0.5 text-[10px] whitespace-nowrap text-white shadow-lg ring-1 ${
                        tooltipMissing ? 'bg-danger/90 ring-danger/30' : 'bg-background ring-border/30'
                      }`}
                      style={{
                        left: `${tooltipX}px`,
                        transform: 'translateX(-50%)',
                      }}
                    >
                      {tooltipText}
                    </div>
                  )}
                </div>
                <DeleteRangeSlider
                  min={globalMin}
                  max={globalMax}
                  start={sliderStart}
                  end={sliderEnd}
                  onChange={handleSliderChange}
                  disabled={isDeleting}
                />
                <div className="flex items-center justify-between text-xs text-muted">
                  <span>{minLabel}</span>
                  <span>{maxLabel}</span>
                </div>
              </div>
            )}

            <div className="grid grid-cols-2 gap-4">
              <div>
                <label htmlFor="delete-start" className="block text-sm font-medium text-muted">
                  Start Position
                </label>
                <input
                  id="delete-start"
                  type="number"
                  value={startPos}
                  onChange={e => handleStartPosChange(e.target.value)}
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
                  onChange={e => handleEndPosChange(e.target.value)}
                  disabled={isDeleting}
                  className={inputClassName}
                  placeholder="0"
                  min={0}
                />
              </div>
            </div>

            <div className="flex items-center gap-4 rounded-lg border border-border/40 bg-secondary/30 px-3 py-2 text-xs text-muted">
              <span>
                Start: <span className="font-medium text-foreground">{startConverted}</span>
              </span>
              <span className="text-border">|</span>
              <span>
                End: <span className="font-medium text-foreground">{endConverted}</span>
              </span>
            </div>

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
