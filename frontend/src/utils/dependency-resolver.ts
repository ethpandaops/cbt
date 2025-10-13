/**
 * Utility for resolving model dependencies with OR group tracking
 */

export interface DependencyWithOrGroups {
  id: string;
  orGroups: number[]; // Which OR groups this dependency belongs to (if any)
}

export interface DependencyResolutionResult {
  dependencies: DependencyWithOrGroups[];
  orGroupMembers: Map<number, string[]>; // Map of OR group ID to all member model IDs
}

interface ResolvedDependency {
  id: string;
  orGroups: Set<number>;
}

/**
 * Resolves dependencies for a model, tracking OR group memberships and maintaining order
 *
 * @param modelId - The model to resolve dependencies for
 * @param dependencyMap - Map of model IDs to their dependencies (supports nested arrays for OR groups)
 * @returns Ordered array of dependencies with OR group information and member map
 */
function resolveDependencies(
  modelId: string,
  dependencyMap: Map<string, Array<string | string[]>>
): DependencyResolutionResult {
  const resolved = new Map<string, ResolvedDependency>();
  const orGroupMembers = new Map<number, string[]>();
  let nextOrGroupId = 1;

  /**
   * Recursively resolve dependencies, tracking OR groups
   */
  function resolveDeps(currentId: string, visited: Set<string> = new Set()): void {
    if (visited.has(currentId)) return;
    visited.add(currentId);

    const deps = dependencyMap.get(currentId);
    if (!deps) return;

    // Process each dependency or OR group
    for (const dep of deps) {
      if (typeof dep === 'string') {
        // Simple AND dependency
        if (!resolved.has(dep)) {
          resolved.set(dep, { id: dep, orGroups: new Set() });
        }
        // Recursively resolve nested dependencies
        resolveDeps(dep, new Set(visited));
      } else {
        // OR group (array of strings)
        const orGroupId = nextOrGroupId++;
        orGroupMembers.set(orGroupId, [...dep]);

        // Process each model in the OR group
        for (const orDep of dep) {
          if (!resolved.has(orDep)) {
            resolved.set(orDep, { id: orDep, orGroups: new Set([orGroupId]) });
          } else {
            // Add this OR group to existing model
            resolved.get(orDep)!.orGroups.add(orGroupId);
          }
          // Recursively resolve nested dependencies
          resolveDeps(orDep, new Set(visited));
        }
      }
    }
  }

  resolveDeps(modelId);

  // Convert to array and sort by name for consistent ordering
  const dependencies = Array.from(resolved.values())
    .map(dep => ({
      id: dep.id,
      orGroups: Array.from(dep.orGroups).sort((a, b) => a - b),
    }))
    .sort((a, b) => a.id.localeCompare(b.id));

  return { dependencies, orGroupMembers };
}

/**
 * Get all dependencies in rendering order with OR group tracking
 */
export function getOrderedDependencies(
  modelId: string,
  dependencyMap: Map<string, Array<string | string[]>>
): DependencyResolutionResult {
  return resolveDependencies(modelId, dependencyMap);
}
