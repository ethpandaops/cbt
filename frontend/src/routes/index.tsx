import { type JSX, useState } from 'react';
import { createFileRoute } from '@tanstack/react-router';
import type { ZoomRanges } from '@/types';
import { IncrementalModelsSection } from '@/components/Domain/Models/IncrementalModelsSection';
import { ScheduledTransformationsSection } from '@/components/Domain/Models/ScheduledTransformationsSection';

function IndexComponent(): JSX.Element {
  // Zoom state for each interval type (keyed by interval type name)
  const [zoomRanges, setZoomRanges] = useState<ZoomRanges>({});

  const handleZoomChange = (intervalType: string, start: number, end: number): void => {
    setZoomRanges(prev => ({
      ...prev,
      [intervalType]: { start, end },
    }));
  };

  return (
    <div className="space-y-8">
      {/* Section 1: Incremental Transformations (Transformations & External) */}
      <section className="relative">
        <div className="mb-8 flex items-center gap-6">
          <div className="relative">
            <div className="absolute inset-0 bg-linear-to-r from-incremental/34 via-accent/30 to-incremental/25 blur-xl" />
            <h2 className="relative bg-linear-to-r from-incremental via-accent to-incremental bg-clip-text text-4xl font-black tracking-tight text-transparent">
              Incremental Transformations
            </h2>
          </div>
          <div className="h-px flex-1 bg-linear-to-r from-incremental/50 via-accent/35 to-transparent" />
        </div>
        <div className="relative">
          <IncrementalModelsSection zoomRanges={zoomRanges} onZoomChange={handleZoomChange} />
        </div>
      </section>

      {/* Section 2: Scheduled Transformation Model Runs */}
      <section className="relative">
        <div className="mb-8 flex items-center gap-6">
          <div className="relative">
            <div className="absolute inset-0 bg-linear-to-r from-scheduled/34 via-accent/28 to-scheduled/25 blur-xl" />
            <h2 className="relative bg-linear-to-r from-scheduled via-accent to-scheduled bg-clip-text text-4xl font-black tracking-tight text-transparent">
              Scheduled Transformations
            </h2>
          </div>
          <div className="h-px flex-1 bg-linear-to-r from-scheduled/50 via-accent/30 to-transparent" />
        </div>
        <div className="relative">
          <ScheduledTransformationsSection />
        </div>
      </section>
    </div>
  );
}

export const Route = createFileRoute('/')({
  component: IndexComponent,
  head: () => ({
    meta: [
      { title: `Dashboard | ${import.meta.env.VITE_BASE_TITLE}` },
      { name: 'description', content: 'Real-time coverage analytics for incremental and scheduled transformations' },
      {
        property: 'og:description',
        content: 'Real-time coverage analytics for incremental and scheduled transformations',
      },
      {
        name: 'twitter:description',
        content: 'Real-time coverage analytics for incremental and scheduled transformations',
      },
    ],
  }),
});
