import {
  ArrowsPointingOutIcon,
  ArrowsRightLeftIcon,
  MagnifyingGlassMinusIcon,
  MagnifyingGlassPlusIcon,
} from '@heroicons/react/24/outline';

export interface ZoomPreset {
  id: 'all' | 'fit' | 'zoom-out' | 'zoom-in';
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
    id: 'zoom-out',
    label: '',
    icon: MagnifyingGlassMinusIcon,
    description: 'Zoom out (larger window)',
  },
  {
    id: 'zoom-in',
    label: '',
    icon: MagnifyingGlassPlusIcon,
    description: 'Zoom in (smaller window)',
  },
];
