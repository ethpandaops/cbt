import { ArrowsPointingOutIcon, ArrowsRightLeftIcon, ClockIcon } from '@heroicons/react/24/outline';

export interface ZoomPreset {
  id: 'all' | 'fit' | 'recent';
  label: string;
  icon: typeof ArrowsPointingOutIcon;
  description: string;
}

export const ZOOM_PRESETS: ZoomPreset[] = [
  {
    id: 'all',
    label: 'All',
    icon: ArrowsPointingOutIcon,
    description: 'Show all available data',
  },
  {
    id: 'fit',
    label: 'Fit',
    icon: ArrowsRightLeftIcon,
    description: 'Fit to incremental models',
  },
  {
    id: 'recent',
    label: 'Recent',
    icon: ClockIcon,
    description: 'Show recent window',
  },
];
