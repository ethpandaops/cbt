import { type JSX, useState } from 'react';
import { createFileRoute } from '@tanstack/react-router';
import type { ZoomRanges } from '@/types';
import { IncrementalModelsSection } from '@/components/IncrementalModelsSection';
import { ScheduledTransformationsSection } from '@/components/ScheduledTransformationsSection';

function IndexComponent(): JSX.Element {
  // Zoom state for each interval type (keyed by interval type name)
  const [zoomRanges, setZoomRanges] = useState<ZoomRanges>({});

  const handleZoomChange = (intervalType: string, start: number, end: number): void => {
    setZoomRanges(prev => ({
      ...prev,
      [intervalType]: { start, end },
    }));
  };

  const handleResetZoom = (intervalType: string): void => {
    setZoomRanges(prev => {
      const newRanges = { ...prev };
      delete newRanges[intervalType];
      return newRanges;
    });
  };

  return (
    <div className="space-y-12">
      {/* Section 1: Incremental Transformations (Transformations & External) */}
      <section>
        <div className="mb-8 flex items-center gap-6">
          <div className="relative">
            <div className="absolute inset-0 bg-gradient-to-r from-indigo-500/30 to-purple-500/30 blur-xl" />
            <h2 className="relative bg-gradient-to-r from-indigo-400 via-purple-400 to-indigo-400 bg-clip-text text-4xl font-black tracking-tight text-transparent">
              Incremental Transformations
            </h2>
          </div>
          <div className="h-px flex-1 bg-gradient-to-r from-indigo-500/40 via-purple-500/40 to-transparent" />
        </div>
        <IncrementalModelsSection
          zoomRanges={zoomRanges}
          onZoomChange={handleZoomChange}
          onResetZoom={handleResetZoom}
        />
      </section>

      {/* Section 2: Scheduled Transformation Model Runs */}
      <section>
        <div className="mb-8 flex items-center gap-6">
          <div className="relative">
            <div className="absolute inset-0 bg-gradient-to-r from-emerald-500/30 to-teal-500/30 blur-xl" />
            <h2 className="relative bg-gradient-to-r from-emerald-400 via-teal-400 to-emerald-400 bg-clip-text text-4xl font-black tracking-tight text-transparent">
              Scheduled Transformations
            </h2>
          </div>
          <div className="h-px flex-1 bg-gradient-to-r from-emerald-500/40 via-teal-500/40 to-transparent" />
        </div>
        <ScheduledTransformationsSection />
      </section>
    </div>
  );
}

export const Route = createFileRoute('/')({
  component: IndexComponent,
});
