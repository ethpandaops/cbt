import { type JSX } from 'react';
import { Tab, TabGroup, TabList } from '@headlessui/react';
import type { IntervalTypeTransformation } from '@/api/types.gen';

export interface TransformationSelectorProps {
  transformations: IntervalTypeTransformation[];
  selectedIndex: number;
  onSelect: (index: number) => void;
  compact?: boolean;
}

export function TransformationSelector({
  transformations,
  selectedIndex,
  onSelect,
  compact = false,
}: TransformationSelectorProps): JSX.Element | null {
  // Only render if there are 2+ transformations
  if (transformations.length < 2) {
    return null;
  }

  return (
    <TabGroup selectedIndex={selectedIndex} onChange={onSelect}>
      <TabList
        className={
          compact
            ? 'inline-flex w-full flex-wrap gap-1 rounded-lg bg-surface/88 p-1 shadow-sm ring-1 ring-border/70 sm:w-auto dark:bg-surface/72 dark:ring-border/65'
            : 'flex flex-wrap gap-1 rounded-lg bg-surface/88 p-1 shadow-sm ring-1 ring-border/70 dark:bg-surface/72 dark:ring-border/65'
        }
      >
        {transformations.map((transformation, index) => (
          <Tab
            key={index}
            className="rounded-md px-2.5 py-1 text-xs font-semibold text-primary/85 transition-all hover:bg-secondary/80 hover:text-primary focus:ring-2 focus:ring-accent/55 focus:outline-hidden data-[selected]:bg-accent data-[selected]:text-white data-[selected]:shadow-sm data-[selected]:ring-1 data-[selected]:shadow-accent/25 data-[selected]:ring-accent/75 sm:px-3 dark:text-foreground/90 dark:hover:bg-secondary/80 dark:hover:text-foreground dark:data-[selected]:bg-accent/45 dark:data-[selected]:text-primary dark:data-[selected]:shadow-accent/35 dark:data-[selected]:ring-accent/80"
            title={transformation.expression || 'No transformation'}
          >
            {transformation.name}
          </Tab>
        ))}
      </TabList>
    </TabGroup>
  );
}
