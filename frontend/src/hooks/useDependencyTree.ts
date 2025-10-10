import { useMemo } from 'react';

export interface DependencyNode {
  id: string;
  depends_on?: string[];
}

export interface UseDependencyTreeReturn {
  allDependencies: Set<string>;
  allDependenciesArray: string[];
  getAllDependencies: (id: string, visited?: Set<string>) => Set<string>;
  getDependenciesArray: (id: string) => string[];
  hasDependencies: boolean;
}

/**
 * Custom hook to calculate dependency tree for a given model
 * @param modelId - The ID of the model to analyze
 * @param dependencyMap - Map of model IDs to their dependencies
 * @returns Object containing all dependencies and helper functions
 */
export function useDependencyTree(modelId: string, dependencyMap: Map<string, string[]>): UseDependencyTreeReturn {
  return useMemo(() => {
    /**
     * Get all dependencies (including transitive) for a model
     */
    const getAllDependencies = (id: string, visited = new Set<string>()): Set<string> => {
      if (visited.has(id)) return new Set();
      visited.add(id);

      const deps = new Set<string>();
      const directDeps = dependencyMap.get(id) || [];

      directDeps.forEach(dep => {
        deps.add(dep);
        const transDeps = getAllDependencies(dep, new Set(visited));
        transDeps.forEach(d => deps.add(d));
      });

      return deps;
    };

    /**
     * Get all dependencies as a flat array
     */
    const getDependenciesArray = (id: string): string[] => {
      return Array.from(getAllDependencies(id));
    };

    const allDependencies = getAllDependencies(modelId);

    return {
      allDependencies,
      allDependenciesArray: Array.from(allDependencies),
      getAllDependencies,
      getDependenciesArray,
      hasDependencies: allDependencies.size > 0,
    };
  }, [modelId, dependencyMap]);
}
