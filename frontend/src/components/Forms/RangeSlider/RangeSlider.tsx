import { type JSX } from 'react';
import RangeSliderInput from 'react-range-slider-input';
import 'react-range-slider-input/dist/style.css';
import './RangeSlider.css';

interface RangeSliderProps {
  globalMin: number;
  globalMax: number;
  zoomStart: number;
  zoomEnd: number;
  onZoomChange: (start: number, end: number) => void;
}

export function RangeSlider({ globalMin, globalMax, zoomStart, zoomEnd, onZoomChange }: RangeSliderProps): JSX.Element {
  return (
    <div className="py-2">
      <RangeSliderInput
        min={globalMin}
        max={globalMax}
        value={[Math.round(zoomStart), Math.round(zoomEnd)]}
        onInput={value => {
          const [start, end] = value as [number, number];
          onZoomChange(start, end);
        }}
      />
    </div>
  );
}
