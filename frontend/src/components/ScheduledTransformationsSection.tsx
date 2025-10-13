import { type JSX, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { listTransformationsOptions, listScheduledRunsOptions } from '@api/@tanstack/react-query.gen';
import { ScheduledTransformationRow } from './ScheduledTransformationRow';
import { LoadingState } from './shared/LoadingState';
import { ErrorState } from './shared/ErrorState';
import { getOrderedDependencies } from '@utils/dependency-resolver';
import type { DependencyWithOrGroups } from '@utils/dependency-resolver';

export function ScheduledTransformationsSection(): JSX.Element {
  const [hoveredModel, setHoveredModel] = useState<string | null>(null);

  // Fetch all transformations (polling handled at root level) and filter client-side
  const allTransformations = useQuery(listTransformationsOptions());
  const scheduledTransformations = {
    ...allTransformations,
    data: allTransformations.data
      ? { ...allTransformations.data, models: allTransformations.data.models.filter(m => m.type === 'scheduled') }
      : undefined,
  };
  const runs = useQuery(listScheduledRunsOptions());

  if (allTransformations.isLoading || runs.isLoading) {
    return <LoadingState />;
  }

  if (scheduledTransformations.error || runs.error) {
    return <ErrorState message={scheduledTransformations.error?.message || runs.error?.message || 'Unknown error'} />;
  }

  // Build dependency map for transformation models with OR group support
  const dependencyMap = new Map<string, Array<string | string[]>>();
  scheduledTransformations.data?.models.forEach(model => {
    if (model.depends_on) {
      dependencyMap.set(model.id, model.depends_on);
    }
  });

  // Build OR group information for each model
  const modelOrGroupInfo = new Map<
    string,
    { dependencies: DependencyWithOrGroups[]; orGroupMembers: Map<number, string[]> }
  >();
  scheduledTransformations.data?.models.forEach(model => {
    if (model.depends_on) {
      const result = getOrderedDependencies(model.id, dependencyMap);
      modelOrGroupInfo.set(model.id, result);
    }
  });

  // Get all dependencies (including transitive) for a model - flattened for highlighting
  const getAllDependencies = (modelId: string, visited = new Set<string>()): Set<string> => {
    if (visited.has(modelId)) return new Set();
    visited.add(modelId);

    const deps = new Set<string>();
    const directDeps = dependencyMap.get(modelId) || [];

    // Flatten OR groups for dependency checking
    directDeps.forEach(dep => {
      if (typeof dep === 'string') {
        deps.add(dep);
      } else {
        dep.forEach(orDep => deps.add(orDep));
      }
    });

    // Recursively get transitive dependencies
    deps.forEach(dep => {
      const transDeps = getAllDependencies(dep, visited);
      transDeps.forEach(d => deps.add(d));
    });

    return deps;
  };

  // Determine which models should be highlighted
  const highlightedModels = new Set<string>();
  if (hoveredModel) {
    highlightedModels.add(hoveredModel);
    const deps = getAllDependencies(hoveredModel);
    deps.forEach(dep => highlightedModels.add(dep));
  }

  // Sort models alphabetically
  const sortedModels = [...(scheduledTransformations.data?.models || [])].sort((a, b) => a.id.localeCompare(b.id));

  return (
    <div className="space-y-2">
      {sortedModels.map(model => {
        const modelRun = runs.data?.runs.find(r => r.id === model.id);
        const isHighlighted = highlightedModels.has(model.id);

        // Find which OR groups this dependency belongs to when another model is hovered
        let orGroupsForDep: number[] | undefined;
        if (hoveredModel && hoveredModel !== model.id) {
          const parentInfo = modelOrGroupInfo.get(hoveredModel);
          if (parentInfo) {
            const depInfo = parentInfo.dependencies.find(d => d.id === model.id);
            if (depInfo && depInfo.orGroups.length > 0) {
              orGroupsForDep = depInfo.orGroups;
            }
          }
        }

        return (
          <ScheduledTransformationRow
            key={model.id}
            model={model}
            lastRun={modelRun?.last_run}
            isHighlighted={isHighlighted}
            isDimmed={false}
            orGroups={orGroupsForDep}
            onMouseEnter={() => setHoveredModel(model.id)}
            onMouseLeave={() => setHoveredModel(null)}
          />
        );
      })}
    </div>
  );
}
