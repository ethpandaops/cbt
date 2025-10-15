import { type JSX } from 'react';
import { Tab, TabGroup, TabList } from '@headlessui/react';
import type { IntervalTypeTransformation } from '@api/types.gen';

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
            ? 'inline-flex w-full flex-wrap gap-1 rounded-lg bg-slate-900/60 p-1 ring-1 ring-slate-700/50 sm:w-auto'
            : 'flex flex-wrap gap-1 rounded-lg bg-slate-900/60 p-1 ring-1 ring-slate-700/50'
        }
      >
        {transformations.map((transformation, index) => (
          <Tab
            key={index}
            className="rounded-md px-2.5 py-1 text-xs font-semibold text-slate-400 transition-all hover:bg-slate-800 hover:text-slate-200 data-[selected]:bg-indigo-500 data-[selected]:text-white data-[selected]:shadow-sm focus:outline-hidden sm:px-3"
            title={transformation.expression || 'No transformation'}
          >
            {transformation.name}
          </Tab>
        ))}
      </TabList>
    </TabGroup>
  );
}
