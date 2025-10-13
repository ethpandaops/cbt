import { describe, it, expect } from 'vitest';
import { getOrderedDependencies } from './dependency-resolver';

describe('dependency-resolver', () => {
  describe('getOrderedDependencies', () => {
    it('should return empty result for model with no dependencies', () => {
      const depMap = new Map([['model.a', []]]);

      const result = getOrderedDependencies('model.a', depMap);

      expect(result.dependencies).toEqual([]);
      expect(result.orGroupMembers.size).toBe(0);
    });

    it('should return empty result for model not in map', () => {
      const depMap = new Map();

      const result = getOrderedDependencies('model.nonexistent', depMap);

      expect(result.dependencies).toEqual([]);
      expect(result.orGroupMembers.size).toBe(0);
    });

    it('should resolve single AND dependency', () => {
      const depMap = new Map([['model.a', ['source.b']]]);

      const result = getOrderedDependencies('model.a', depMap);

      expect(result.dependencies).toEqual([{ id: 'source.b', orGroups: [] }]);
      expect(result.orGroupMembers.size).toBe(0);
    });

    it('should resolve multiple AND dependencies', () => {
      const depMap = new Map([['model.a', ['source.b', 'source.c', 'source.d']]]);

      const result = getOrderedDependencies('model.a', depMap);

      expect(result.dependencies).toHaveLength(3);
      expect(result.dependencies).toEqual([
        { id: 'source.b', orGroups: [] },
        { id: 'source.c', orGroups: [] },
        { id: 'source.d', orGroups: [] },
      ]);
      expect(result.orGroupMembers.size).toBe(0);
    });

    it('should resolve single OR group', () => {
      const depMap = new Map([['model.a', [['source.b', 'source.c']]]]);

      const result = getOrderedDependencies('model.a', depMap);

      expect(result.dependencies).toHaveLength(2);
      expect(result.dependencies).toEqual([
        { id: 'source.b', orGroups: [1] },
        { id: 'source.c', orGroups: [1] },
      ]);
      expect(result.orGroupMembers.get(1)).toEqual(['source.b', 'source.c']);
    });

    it('should resolve mixed AND and OR dependencies', () => {
      const depMap = new Map([
        ['model.a', ['source.required', ['source.option1', 'source.option2'], 'source.also_required']],
      ]);

      const result = getOrderedDependencies('model.a', depMap);

      expect(result.dependencies).toHaveLength(4);
      expect(result.dependencies).toEqual([
        { id: 'source.also_required', orGroups: [] },
        { id: 'source.option1', orGroups: [1] },
        { id: 'source.option2', orGroups: [1] },
        { id: 'source.required', orGroups: [] },
      ]);
      expect(result.orGroupMembers.get(1)).toEqual(['source.option1', 'source.option2']);
    });

    it('should resolve multiple OR groups', () => {
      const depMap = new Map([
        [
          'model.a',
          [
            ['source.a1', 'source.a2'],
            ['source.b1', 'source.b2'],
          ],
        ],
      ]);

      const result = getOrderedDependencies('model.a', depMap);

      expect(result.dependencies).toHaveLength(4);
      expect(result.dependencies).toEqual([
        { id: 'source.a1', orGroups: [1] },
        { id: 'source.a2', orGroups: [1] },
        { id: 'source.b1', orGroups: [2] },
        { id: 'source.b2', orGroups: [2] },
      ]);
      expect(result.orGroupMembers.get(1)).toEqual(['source.a1', 'source.a2']);
      expect(result.orGroupMembers.get(2)).toEqual(['source.b1', 'source.b2']);
    });

    it('should handle nested dependencies (transitive)', () => {
      const depMap = new Map([
        ['model.a', ['model.b']],
        ['model.b', ['source.c']],
      ]);

      const result = getOrderedDependencies('model.a', depMap);

      expect(result.dependencies).toHaveLength(2);
      expect(result.dependencies).toEqual([
        { id: 'model.b', orGroups: [] },
        { id: 'source.c', orGroups: [] },
      ]);
    });

    it('should handle nested dependencies with OR groups', () => {
      const depMap = new Map([
        ['model.a', [['model.b', 'model.c']]],
        ['model.b', ['source.d']],
        ['model.c', ['source.e']],
      ]);

      const result = getOrderedDependencies('model.a', depMap);

      expect(result.dependencies).toHaveLength(4);
      // model.b and model.c should be in OR group 1
      // source.d and source.e are transitive dependencies (not in OR groups)
      expect(result.dependencies).toEqual([
        { id: 'model.b', orGroups: [1] },
        { id: 'model.c', orGroups: [1] },
        { id: 'source.d', orGroups: [] },
        { id: 'source.e', orGroups: [] },
      ]);
      expect(result.orGroupMembers.get(1)).toEqual(['model.b', 'model.c']);
    });

    it('should avoid circular dependencies', () => {
      const depMap = new Map([
        ['model.a', ['model.b']],
        ['model.b', ['model.a']], // Circular!
      ]);

      const result = getOrderedDependencies('model.a', depMap);

      // Should include both but not infinitely recurse
      expect(result.dependencies).toHaveLength(2);
      expect(result.dependencies).toEqual([
        { id: 'model.a', orGroups: [] },
        { id: 'model.b', orGroups: [] },
      ]);
    });

    it('should handle model appearing in multiple OR groups', () => {
      const depMap = new Map([
        [
          'model.a',
          [
            ['source.x', 'source.common'],
            ['source.y', 'source.common'],
          ],
        ],
      ]);

      const result = getOrderedDependencies('model.a', depMap);

      // source.common should be in both OR groups
      expect(result.dependencies).toHaveLength(3);
      const commonDep = result.dependencies.find(d => d.id === 'source.common');
      expect(commonDep).toBeDefined();
      expect(commonDep?.orGroups).toEqual([1, 2]);

      expect(result.orGroupMembers.get(1)).toEqual(['source.x', 'source.common']);
      expect(result.orGroupMembers.get(2)).toEqual(['source.y', 'source.common']);
    });

    it('should sort dependencies alphabetically by id', () => {
      const depMap = new Map([['model.a', ['zebra.source', 'apple.source', 'middle.source']]]);

      const result = getOrderedDependencies('model.a', depMap);

      expect(result.dependencies.map(d => d.id)).toEqual(['apple.source', 'middle.source', 'zebra.source']);
    });

    it('should handle complex real-world scenario', () => {
      // Example from the OpenAPI spec
      const depMap = new Map([
        [
          'analytics.report',
          [
            'ethereum.beacon_blocks', // Required
            ['source1.data', 'source2.data'], // OR group 1
            'analytics.hourly_stats', // Required
          ],
        ],
        ['analytics.hourly_stats', ['ethereum.beacon_blocks']], // Nested dependency
      ]);

      const result = getOrderedDependencies('analytics.report', depMap);

      expect(result.dependencies).toHaveLength(4);
      expect(result.dependencies).toEqual([
        { id: 'analytics.hourly_stats', orGroups: [] },
        { id: 'ethereum.beacon_blocks', orGroups: [] },
        { id: 'source1.data', orGroups: [1] },
        { id: 'source2.data', orGroups: [1] },
      ]);
      expect(result.orGroupMembers.get(1)).toEqual(['source1.data', 'source2.data']);
    });

    it('should handle empty OR group (edge case)', () => {
      const depMap = new Map([['model.a', [[]]]]);

      const result = getOrderedDependencies('model.a', depMap);

      // Empty OR group should create a group but with no members
      expect(result.dependencies).toEqual([]);
      expect(result.orGroupMembers.get(1)).toEqual([]);
    });

    it('should handle deeply nested dependencies', () => {
      const depMap = new Map([
        ['level1', ['level2']],
        ['level2', ['level3']],
        ['level3', ['level4']],
        ['level4', ['level5']],
        ['level5', []],
      ]);

      const result = getOrderedDependencies('level1', depMap);

      expect(result.dependencies).toHaveLength(4);
      expect(result.dependencies.map(d => d.id).sort()).toEqual(['level2', 'level3', 'level4', 'level5']);
    });

    it('should handle DAG (directed acyclic graph) structure', () => {
      const depMap = new Map([
        ['root', ['branch1', 'branch2']],
        ['branch1', ['leaf1', 'leaf2']],
        ['branch2', ['leaf2', 'leaf3']], // leaf2 is shared
      ]);

      const result = getOrderedDependencies('root', depMap);

      expect(result.dependencies).toHaveLength(5);
      // Should include each unique dependency once
      const ids = result.dependencies.map(d => d.id);
      expect(ids).toContain('branch1');
      expect(ids).toContain('branch2');
      expect(ids).toContain('leaf1');
      expect(ids).toContain('leaf2');
      expect(ids).toContain('leaf3');
      // No duplicates
      expect(ids.length).toBe(5);
    });
  });

  describe('real-world integration scenarios', () => {
    it('should match expected format from OpenAPI spec example', () => {
      // From openapi.yaml:583-599
      const depMap = new Map([
        [
          'analytics.daily_active_users',
          ['ethereum.beacon_blocks', ['source1.data', 'source2.data'], 'analytics.hourly_stats'],
        ],
      ]);

      const result = getOrderedDependencies('analytics.daily_active_users', depMap);

      // Verify structure matches what the frontend expects
      expect(result).toHaveProperty('dependencies');
      expect(result).toHaveProperty('orGroupMembers');
      expect(Array.isArray(result.dependencies)).toBe(true);
      expect(result.orGroupMembers instanceof Map).toBe(true);

      // Verify each dependency has correct shape
      result.dependencies.forEach(dep => {
        expect(dep).toHaveProperty('id');
        expect(dep).toHaveProperty('orGroups');
        expect(typeof dep.id).toBe('string');
        expect(Array.isArray(dep.orGroups)).toBe(true);
      });
    });

    it('should support dependency visualization use case', () => {
      // Test that results can be used to build a dependency graph
      const depMap = new Map([
        ['final.model', [['option.a', 'option.b'], 'required.c']],
        ['required.c', ['base.source']],
      ]);

      const result = getOrderedDependencies('final.model', depMap);

      // Can build edges for visualization
      const edges: Array<{ from: string; to: string; orGroup?: number }> = [];

      result.dependencies.forEach(dep => {
        if (dep.orGroups.length > 0) {
          dep.orGroups.forEach(groupId => {
            edges.push({ from: 'final.model', to: dep.id, orGroup: groupId });
          });
        } else {
          edges.push({ from: 'final.model', to: dep.id });
        }
      });

      expect(edges.length).toBeGreaterThan(0);
      expect(edges.some(e => e.orGroup !== undefined)).toBe(true);
      expect(edges.some(e => e.orGroup === undefined)).toBe(true);
    });
  });
});
