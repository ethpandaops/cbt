import { type JSX } from 'react';
import RangeSliderInput from 'react-range-slider-input';
import 'react-range-slider-input/dist/style.css';
import './DeleteRangeSlider.css';

interface DeleteRangeSliderProps {
  min: number;
  max: number;
  start: number;
  end: number;
  onChange: (start: number, end: number) => void;
  disabled?: boolean;
}

export function DeleteRangeSlider({ min, max, start, end, onChange, disabled }: DeleteRangeSliderProps): JSX.Element {
  return (
    <div className="delete-range-slider py-2">
      <RangeSliderInput
        min={min}
        max={max}
        value={[Math.round(start), Math.round(end)]}
        onInput={value => {
          const [s, e] = value as [number, number];
          onChange(s, e);
        }}
        disabled={disabled}
      />
    </div>
  );
}
