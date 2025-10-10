import { type JSX, useEffect, useState, useRef } from 'react';
import { createPortal } from 'react-dom';
import type { IncrementalModelItem } from '@/types';
import type { IntervalTypeTransformation } from '@api/types.gen';
import { findCoverageAtPosition, findGapAtPosition, mergeRanges } from '@/utils/coverage-helpers';
import { transformValue, formatValue } from '@/utils/interval-transform';

interface TooltipData {
  modelId: string;
  content: string;
  rowTop: number;
  barHeight: number;
  positionX: number;
  isMissing?: boolean;
  containerWidth?: number;
  coverageBarLeft?: number;
  coverageBarRight?: number;
}

export interface MultiCoverageTooltipProps {
  hoveredPosition: number;
  hoveredModelId: string;
  mouseX: number; // Actual mouse X position in pixels
  allModels: IncrementalModelItem[];
  dependencyIds: Set<string>;
  transformation?: IntervalTypeTransformation;
  zoomStart: number;
  zoomEnd: number;
  containerRef: React.RefObject<HTMLElement | null>;
}

export function MultiCoverageTooltip({
  hoveredPosition,
  hoveredModelId,
  mouseX,
  allModels,
  dependencyIds,
  transformation,
  zoomStart,
  zoomEnd,
  containerRef,
}: MultiCoverageTooltipProps): JSX.Element | null {
  const [tooltipsData, setTooltipsData] = useState<TooltipData[]>([]);
  const tooltipRefs = useRef<Map<string, HTMLDivElement>>(new Map());

  useEffect(() => {
    if (!containerRef.current) return;

    const tooltips: TooltipData[] = [];

    // All models we should show tooltips for (hovered + dependencies)
    const modelsToShow = [hoveredModelId, ...Array.from(dependencyIds)];

    modelsToShow.forEach(modelId => {
      const model = allModels.find(m => m.id === modelId);
      if (!model) {
        return;
      }

      // Find the model row element
      const rowElement = document.querySelector(`[data-model-id="${modelId}"]`);
      if (!rowElement) {
        return;
      }

      const containerRect = containerRef.current!.getBoundingClientRect();

      // Find the coverage bar element within the row
      const coverageBarElement = rowElement.querySelector('[style*="height"]');
      const coverageBarRect = coverageBarElement?.getBoundingClientRect();

      // Use the coverage bar position, not the row position
      const barTop = coverageBarRect?.top || rowElement.getBoundingClientRect().top;

      // Find coverage at this position
      let coverageContent: string | null = null;

      if (model.type === 'transformation' && model.data.coverage) {
        const merged = mergeRanges(model.data.coverage);
        const coverage = findCoverageAtPosition(merged, hoveredPosition);

        if (coverage) {
          const rangeMin = coverage.position;
          const rangeMax = coverage.position + coverage.interval;

          if (transformation) {
            const formattedMin = formatValue(transformValue(rangeMin, transformation), transformation.format);
            const formattedMax = formatValue(transformValue(rangeMax, transformation), transformation.format);
            coverageContent = `${formattedMin} → ${formattedMax}`;
          } else {
            coverageContent = `${rangeMin.toLocaleString()} → ${rangeMax.toLocaleString()}`;
          }
        }
      } else if (model.type === 'external' && model.data.bounds) {
        const { min, max } = model.data.bounds;

        if (hoveredPosition >= min && hoveredPosition <= max) {
          if (transformation) {
            const formattedMin = formatValue(transformValue(min, transformation), transformation.format);
            const formattedMax = formatValue(transformValue(max, transformation), transformation.format);
            coverageContent = `${formattedMin} → ${formattedMax}`;
          } else {
            coverageContent = `${min.toLocaleString()} → ${max.toLocaleString()}`;
          }
        }
      }

      // Always show tooltip - either coverage or "Missing"
      if (coverageContent) {
        tooltips.push({
          modelId,
          content: coverageContent,
          rowTop: barTop + window.scrollY,
          barHeight: coverageBarRect?.height || 24,
          positionX: mouseX,
          isMissing: false,
          containerWidth: containerRect.width,
          coverageBarLeft: coverageBarRect?.left,
          coverageBarRight: coverageBarRect?.right,
        });
      } else {
        // Show "Missing" tooltip with gap range
        let missingContent: string;

        if (model.type === 'transformation' && model.data.coverage) {
          const gap = findGapAtPosition(model.data.coverage, hoveredPosition, zoomStart, zoomEnd);
          if (gap) {
            const gapMin = gap.position;
            const gapMax = gap.position + gap.interval;
            missingContent = transformation
              ? `Missing: ${formatValue(transformValue(gapMin, transformation), transformation.format)} → ${formatValue(transformValue(gapMax, transformation), transformation.format)}`
              : `Missing: ${gapMin.toLocaleString()} → ${gapMax.toLocaleString()}`;
          } else {
            missingContent = 'Missing';
          }
        } else if (model.type === 'external' && model.data.bounds) {
          const { min, max } = model.data.bounds;
          // For external models, show the entire bounds as missing
          missingContent = transformation
            ? `Missing: ${formatValue(transformValue(min, transformation), transformation.format)} → ${formatValue(transformValue(max, transformation), transformation.format)}`
            : `Missing: ${min.toLocaleString()} → ${max.toLocaleString()}`;
        } else {
          missingContent = transformation
            ? `Missing: ${formatValue(transformValue(hoveredPosition, transformation), transformation.format)}`
            : `Missing: ${hoveredPosition.toLocaleString()}`;
        }

        tooltips.push({
          modelId,
          content: missingContent,
          rowTop: barTop + window.scrollY,
          barHeight: coverageBarRect?.height || 24,
          positionX: mouseX,
          isMissing: true,
          containerWidth: containerRect.width,
          coverageBarLeft: coverageBarRect?.left,
          coverageBarRight: coverageBarRect?.right,
        });
      }
    });

    setTooltipsData(tooltips);
  }, [
    hoveredPosition,
    hoveredModelId,
    mouseX,
    allModels,
    dependencyIds,
    transformation,
    zoomStart,
    zoomEnd,
    containerRef,
  ]);

  if (tooltipsData.length === 0) return null;

  return createPortal(
    <div className="pointer-events-none fixed inset-0 z-50">
      {tooltipsData.map(tooltip => {
        const isHovered = tooltip.modelId === hoveredModelId;
        const tooltipEl = tooltipRefs.current.get(tooltip.modelId);

        let transform = 'translate(-50%, 0)';
        let finalPositionX = tooltip.positionX;

        if (isHovered && tooltip.containerWidth) {
          // For hovered tooltip, check if it would go outside the container on the right
          const tooltipWidth = tooltipEl?.offsetWidth || 150;
          const containerRight = (containerRef.current?.getBoundingClientRect().left || 0) + tooltip.containerWidth;
          const wouldOverflowRight = tooltip.positionX + 15 + tooltipWidth > containerRight;

          if (wouldOverflowRight) {
            transform = 'translate(calc(-100% - 15px), 0)'; // Show on left
          } else {
            transform = 'translate(15px, 0)'; // Show on right
          }
        } else if (!isHovered && tooltip.coverageBarLeft !== undefined && tooltip.coverageBarRight !== undefined) {
          // For dependency tooltips, constrain within coverage bar bounds
          const tooltipWidth = tooltipEl?.offsetWidth || 150;
          const halfTooltipWidth = tooltipWidth / 2;

          // Clamp position to stay within bar bounds
          const minX = tooltip.coverageBarLeft + halfTooltipWidth;
          const maxX = tooltip.coverageBarRight - halfTooltipWidth;
          finalPositionX = Math.max(minX, Math.min(maxX, tooltip.positionX));
        }

        return (
          <div
            key={tooltip.modelId}
            ref={el => {
              if (el) tooltipRefs.current.set(tooltip.modelId, el);
            }}
            className={`absolute whitespace-nowrap rounded px-2 py-1 text-xs text-white opacity-100 shadow-lg ring-1 ${
              tooltip.isMissing ? 'bg-red-900/90 ring-red-500/30' : 'bg-gray-900 ring-white/10'
            }`}
            style={{
              top: tooltip.rowTop + tooltip.barHeight / 2,
              left: finalPositionX,
              transform: `${transform} translateY(-50%)`,
            }}
          >
            <div className={`font-semibold ${tooltip.isMissing ? 'text-red-300' : 'text-indigo-300'}`}>
              {tooltip.modelId}
            </div>
            <div>{tooltip.content}</div>
          </div>
        );
      })}
    </div>,
    document.body
  );
}
